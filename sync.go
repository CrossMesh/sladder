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
		watchEntries = append(watchEntries, c.getRealEntry(kv))
	}

	names, err := c.resolver.Resolve(watchEntries...)
	if err != nil {
		return nil, err
	}

	return names, nil
}

func (t *Transaction) mergeNode(target, source *Node) (merged bool, err error) {
	t.lockRelatedNode(source)
	if err = t.mergeNodeEntries(target, false, false, false, func(fill func(*KeyValue) bool) {
		for _, entries := range source.kvs {
			if !fill(&entries.KeyValue) {
				break
			}
		}
	}); err != nil {
		return false, err
	}
	merged, err = t.RemoveNode(source)
	return
}

func (t *Transaction) mergeNodeEntries(n *Node, deletion, failMissingValidator, failSyncFailure bool, iterateEntries func(func(*KeyValue) bool)) (err error) {
	type diffLog struct {
		log                     *txnLog
		value                   string
		isDelete, originDeleted bool
	}

	if n == nil {
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

	syncEntry := func(entry *KeyValue, validator KVValidator, msgKV *KeyValue) (accepted bool, err error) {
		accepted, err = validator.Sync(entry, msgKV)
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
			t.Cluster.log.Error(err.Error())
			return log, err
		}
		return log, nil
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	iterateEntries(func(entry *KeyValue) bool {
		key := entry.Key

		if log, err = getLatestLog(key); err != nil {
			return false
		}
		if log == nil {
			return true
		}

		latestValue := log.txn.After()

		buf := KeyValue{Key: key, Value: latestValue}
		if accepted, err = syncEntry(&buf, log.validator, entry); err != nil {
			return false
		} else if accepted {
			// save diff.
			syncLogs = append(syncLogs, &diffLog{log: log, value: buf.Value})
		}

		if deletion {
			existingKeySet[key] = struct{}{}
		}
		return true
	})

	if err != nil {
		return err
	}

	if deletion {
		t.rangeNodeKeys(n, func(key string, pastExists bool) bool {
			if _, exists := existingKeySet[key]; !exists {
				if log, err = getLatestLog(key); err != nil {
					return false
				}
				latestValue := log.txn.After()
				buf := KeyValue{Key: key, Value: latestValue}
				if accepted, err = syncEntry(&buf, log.validator, nil); err != nil {
					return false
				} else if !accepted {
					return true
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
				// failed to rollback. transaction is broken.
				t.errs = append(t.errs, err, ierr, ErrTransactionStateBroken)
				return t.errs.AsError()
			}
		}
	}

	return nil
}

// MergeNodeSnapshot updates node entries by refering to protobuf snapshot.
func (t *Transaction) MergeNodeSnapshot(n *Node, s *proto.Node, deletion, failMissingValidator, failSyncFailure bool) (err error) {
	if n == nil || s == nil {
		return nil
	}
	return t.mergeNodeEntries(n, deletion, failMissingValidator, failSyncFailure, func(fill func(*KeyValue) bool) {
		for _, msg := range s.Kvs {
			if !fill(&KeyValue{
				Key: msg.Key, Value: msg.Value,
			}) {
				break
			}
		}
	})
}
