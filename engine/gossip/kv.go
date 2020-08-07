package gossip

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/sunmxt/sladder"
	"github.com/sunmxt/sladder/util"
)

func (e *EngineInstance) enforceTransactionCommitLimit(t *sladder.Transaction, isEngineTxn bool, ops []*sladder.TransactionOperation) (bool, error) {
	if len(ops) < 1 {
		return true, nil
	}

	type nodeOpMeta struct {
		swim   *SWIMTagTxn
		oldTag *SWIMTag
	}

	metas := make(map[*sladder.Node]*nodeOpMeta)
	self := t.Cluster.Self()

	// enforce SWIM tag modification rules.
	for _, op := range ops {
		if op.Txn == nil { // ignore node operation.
			continue
		}

		meta, _ := metas[op.Node]
		if meta == nil {
			meta = &nodeOpMeta{}
			metas[op.Node] = meta
		}
		if op.Key == e.swimTagKey {
			rtx, err := t.KV(op.Node, e.swimTagKey)
			if err != nil {
				e.log.Fatalf("engine cannot update entry list in swim tag. (err = \"%v\")", err)
				return false, err
			}
			meta.swim = rtx.(*SWIMTagTxn)
			meta.oldTag = &SWIMTag{}
			if err := meta.oldTag.Decode(meta.swim.Before()); err != nil {
				e.log.Fatalf("failed to decode old SWIM tag. (err = \"%v\")", err)
				return false, err
			}

			if !op.Node.Anonymous() && // no rule for anonymous node
				!isEngineTxn && // engine coordinator is allowed to do any operation.
				self != op.Node { // self can apply any operation.
				if op.PastExists == op.Exists {
					if op.Updated { // seem to be modified.
						modified := len(meta.swim.EntryList(false)) != len(meta.oldTag.EntryList)
						if !modified {
							// rule: modification of entry list is not allowed.
							mark := func(s *string) bool { modified = true; return true }
							util.RangeOverStringSortedSet(meta.swim.EntryList(false), meta.oldTag.EntryList, mark, mark, nil)
						}
						if modified {
							return false, sladder.ErrTransactionCommitViolation
						}
					}
				} else { // delete or new.
					return false, sladder.ErrTransactionCommitViolation
				}
			}

		}
	}

	// enforce entry rules.
	for node, meta := range metas {
		if node == self || // exclude self.
			!node.Anonymous() { // no rule for anonymous node
			continue
		}

		var allowedKeys []string
		var passiveDeletedKeys []string
		var swim *SWIMTagTxn

		if meta.swim == nil {
			rtx, err := t.KV(node, e.swimTagKey)
			if err != nil {
				e.log.Fatalf("engine cannot update entry list in swim tag. (err = \"%v\")", err)
				return false, err
			}
			swim = rtx.(*SWIMTagTxn)
		} else {
			swim = meta.swim
		}
		allowedKeys = swim.EntryList(false)

		rejectKey := ""
		t.RangeNodeKeys(node, func(key string, pastExists bool) bool {
			if key == e.swimTagKey {
				return true
			}
			if sort.SearchStrings(allowedKeys, key) < 0 { // forbidden key.
				if pastExists { // passive deletion.
					passiveDeletedKeys = append(passiveDeletedKeys, key)
				} else { // user deletion. reject it.
					rejectKey = key
					return false
				}
			}
			return true
		})
		if rejectKey != "" { // user add invalid key.
			return false, fmt.Errorf("%v. {key = \"%v\", node = \"%v\"}", sladder.ErrTransactionCommitViolation, rejectKey, node.PrintableName())
		}

		// deal with passive deletions.
		for _, key := range passiveDeletedKeys {
			t.Delete(node, key)
		}
	}

	return true, nil
}

type wrapVersionKVValidator struct {
	log            sladder.Logger
	ov             sladder.KVValidator
	extendedSyncer sladder.KVExtendedSyncer
}

// WrapVersionKVValidator wraps existing validator to make versioned data model.
func WrapVersionKVValidator(v sladder.KVValidator, log sladder.Logger) sladder.KVValidator {
	if log == nil {
		log = sladder.DefaultLogger
	}
	syncer, _ := v.(sladder.KVExtendedSyncer)
	return wrapVersionKVValidator{
		ov:             v,
		log:            log,
		extendedSyncer: syncer,
	}
}

// WrapVersionKVValidator wraps existing validator to make versioned data model.
func (e *EngineInstance) WrapVersionKVValidator(v sladder.KVValidator) sladder.KVValidator {
	return WrapVersionKVValidator(v, e.log)
}

type wrapVersionKV struct {
	Value   string `json:"o"`
	Version uint32 `json:"v"`
}

func (v *wrapVersionKV) Encode() (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (v *wrapVersionKV) Decode(x string) error {
	if x == "" {
		x = "{}"
	}
	return json.Unmarshal([]byte(x), v)
}

type wrapVersionKVSyncProperties struct {
	isConcurrent bool
}

func (p *wrapVersionKVSyncProperties) Concurrent() bool           { return p.isConcurrent }
func (p *wrapVersionKVSyncProperties) Get(key string) interface{} { return nil }

type wrapVersionKVTxn struct {
	oldVersion, version uint32
	o                   sladder.KVTransaction
}

// After returns new value wrapped with version.
func (t *wrapVersionKVTxn) After() (changed bool, new string) {
	changed, new = t.o.After()
	wrap := &wrapVersionKV{Value: new}
	if changed {
		if t.version > t.oldVersion {
			wrap.Version = t.version
		} else {
			wrap.Version = t.oldVersion + 1
		}
	} else {
		wrap.Version = t.oldVersion
	}

	var err error
	if new, err = wrap.Encode(); err != nil {
		panic(err)
	}

	return
}

// Before return origin raw value wrapped with version.
func (t *wrapVersionKVTxn) Before() string {
	ori, err := (&wrapVersionKV{Value: t.o.Before(), Version: t.oldVersion}).Encode()
	if err != nil {
		panic(err)
	}
	return ori
}

func (t *wrapVersionKVTxn) SetRawValue(x string) error {
	tag := wrapVersionKV{}
	if err := tag.Decode(x); err != nil {
		return err
	}
	if err := t.o.SetRawValue(tag.Value); err != nil {
		return err
	}
	t.version = tag.Version
	return nil
}

func (t *wrapVersionKVTxn) KVTransaction() sladder.KVTransaction { return t.o }

func (v wrapVersionKVValidator) Sync(rlocal *sladder.KeyValue, rremote *sladder.KeyValue) (changed bool, err error) {
	// local
	if rlocal == nil {
		return false, nil
	}
	local := wrapVersionKV{}
	if err := local.Decode(rlocal.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Sync() got invalid local value. err = \"%v\"", err)
		return false, err
	}

	props := wrapVersionKVSyncProperties{}

	sync := func(local, remote *sladder.KeyValue) (changed bool, err error) {
		if v.extendedSyncer != nil {
			return v.extendedSyncer.SyncEx(local, remote, &props)
		}
		if remote == nil ||
			!props.Concurrent() ||
			remote.Value >= local.Value {
			return v.ov.Sync(local, remote)
		}
		return false, nil
	}

	// remote
	if rremote == nil { // a deletion
		return sync(&sladder.KeyValue{
			Key: rlocal.Key, Value: local.Value,
		}, nil)
	}
	remote := wrapVersionKV{}
	if err := remote.Decode(rremote.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Sync() got invalid remote value. err = \"%v\"", err)
		return false, err
	}

	if remote.Version < local.Version { // reject old version.
		return false, nil
	}
	props.isConcurrent = remote.Version == local.Version

	// normal sync.
	obuf, cbuf := &sladder.KeyValue{Key: rlocal.Key, Value: local.Value}, &sladder.KeyValue{Key: rremote.Key, Value: remote.Value}
	if changed, err = sync(obuf, cbuf); err != nil {
		return false, err
	}
	if !changed && local.Value != obuf.Value {
		changed = true
	}
	if changed {
		//fmt.Printf("wrapper merge: ver = %v, val = %v <-- ver = %v, val = %v \n", local.Version, local.Value, remote.Version, obuf.Value)
		// save changes.
		local.Value, local.Version = obuf.Value, remote.Version
		new, ierr := local.Encode()
		if ierr != nil {
			return false, ierr
		}
		rlocal.Value = new
	}

	return
}

func (v wrapVersionKVValidator) Validate(x sladder.KeyValue) bool {
	wrap := wrapVersionKV{}
	if err := wrap.Decode(x.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Validate() decoding got invalid value. err = \"%v\"", err)
		return false
	}
	return v.ov.Validate(sladder.KeyValue{
		Key: x.Key, Value: wrap.Value,
	})
}

func (v wrapVersionKVValidator) Txn(x sladder.KeyValue) (txn sladder.KVTransaction, err error) {
	wrapTxn := &wrapVersionKVTxn{}

	wrap := &wrapVersionKV{}
	if err := wrap.Decode(x.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Txn() decoding got invalid value. err = \"%v\"", err)
		return nil, err
	}

	if txn, err = v.ov.Txn(sladder.KeyValue{
		Key: x.Key, Value: wrap.Value,
	}); err != nil {
		return nil, err
	}
	wrapTxn.oldVersion = wrap.Version
	wrapTxn.version = wrapTxn.oldVersion
	wrapTxn.o = txn

	return wrapTxn, nil
}
