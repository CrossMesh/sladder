package sladder

import (
	"fmt"
	"sync/atomic"

	"github.com/crossmesh/sladder/proto"
)

// ResolveNodeNameFromProtobuf resolves node names from node snapshot.
func (c *Cluster) resolveNodeNameFromProtobuf(entries []*proto.Node_KeyValue) ([]string, error) {
	watchKeySet := make(map[string]*KeyValue)
	for _, key := range c.resolver.Keys() {
		watchKeySet[key] = nil
	}

	for _, entry := range entries {
		kv, watched := watchKeySet[entry.Key]
		if !watched {
			continue
		}
		if kv == nil {
			watchKeySet[entry.Key] = &KeyValue{
				Key:   entry.Key,
				Value: entry.Value,
			}
		}
	}

	watchEntries := make([]*KeyValue, 0, len(watchKeySet))
	for _, kv := range watchKeySet {
		if kv == nil {
			continue
		}
		watchEntries = append(watchEntries, kv)
	}

	names, err := c.resolver.Resolve(watchEntries...)
	if err != nil {
		return nil, err
	}

	return names, nil
}

// MergeNodeSnapshot updates node entries by refering to protobuf snapshot.
func (t *Transaction) MergeNodeSnapshot(n *Node, s *proto.Node, deletion, failMissingValidator, failSyncFailure bool) (err error) {
	type diffLog struct {
		log                     *txnLog
		value                   string
		isDelete, originDeleted bool
	}

	if n == nil || s == nil {
		return nil
	}

	var (
		accepted       bool
		syncLogs       []*diffLog
		log            *txnLog
		existingKeySet map[string]struct{}
	)

	if deletion {
		existingKeySet = make(map[string]struct{})
	}

	syncEntry := func(entry *KeyValue, validator KVValidator, msgKV *proto.Node_KeyValue) (accepted bool, err error) {
		if msgKV == nil {
			accepted, err = validator.Sync(entry, nil)
		} else {
			accepted, err = validator.Sync(entry, &KeyValue{
				Key:   msgKV.Key,
				Value: msgKV.Value,
			})
		}
		if err != nil {
			if !failSyncFailure {
				err = nil
			}
			return false, err
		}
		return accepted, nil
	}

	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.

	getLatestLog := func(key string) (log *txnLog, err error) {
		if log, _, err = t.getLatestLog(n, key, true, lc); err != nil {
			if err != ErrValidatorMissing || failMissingValidator {
				return nil, err
			}
		}
		if log.txn == nil {
			err = fmt.Errorf("Got nil KVTransaction in op log. {key = %v, node = %v}", key, n.PrintableName())
			t.Cluster.log.Fatal(err.Error())
			return log, err
		}
		return log, nil
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	for _, msg := range s.Kvs {
		key := msg.Key

		if log, err = getLatestLog(key); err != nil {
			return err
		}
		if log == nil {
			continue
		}

		latestValue := log.txn.After()

		buf := KeyValue{Key: key, Value: latestValue}
		if accepted, err = syncEntry(&buf, log.validator, msg); err != nil {
			return err
		} else if accepted {
			// save diff.
			syncLogs = append(syncLogs, &diffLog{log: log, value: buf.Value})
		}

		if deletion {
			existingKeySet[key] = struct{}{}
		}
	}

	if deletion {
		t.rangeNodeKeys(n, func(key string, pastExists bool) bool {
			if _, exists := existingKeySet[key]; !exists {
				if log, err = getLatestLog(key); err != nil {
					return false
				}
				syncLogs = append(syncLogs, &diffLog{log: log, isDelete: true})
			}
			return true
		})
		if err != nil {
			return err
		}
	}

	// TODO(xutao): better to validate values first.
	// apply sync diffs.
	lastLog := 0
	for lastLog < len(syncLogs) {
		var new string

		diff := syncLogs[lastLog]
		old := diff.log.txn.After()
		if !diff.isDelete {
			new = diff.value
		} else {
			new = diff.log.txn.Before()
			diff.originDeleted = diff.log.deletion
			diff.log.deletion = true
		}
		if err = diff.log.txn.SetRawValue(new); err != nil { // fatal. value invalid.
			err = fmt.Errorf("merge snapshot fails to apply raw value. (err = \"%v\") {key = \"%v\", node = \"%v\"}", err, diff, n.PrintableName())
			break
		}
		diff.value = old

		lastLog++
	}

	if err != nil { // rollback log states.
		for rollIdx := 0; rollIdx < lastLog; rollIdx++ {
			diff := syncLogs[rollIdx]
			if diff.isDelete {
				diff.log.deletion = diff.originDeleted
			}
			if ierr := diff.log.txn.SetRawValue(diff.value); ierr != nil {
				t.errs = append(t.errs, err, ierr, ErrTransactionStateBroken)
				return t.errs.AsError()
			}
		}
	}

	return nil
}
