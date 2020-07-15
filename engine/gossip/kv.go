package gossip

import (
	"encoding/json"
	"errors"

	"github.com/sunmxt/sladder"
)

var (
	// ErrTransactionCommitViolation raises when any operation breaks commit limitation.
	ErrTransactionCommitViolation = errors.New("modification violation in transactiom commit")
)

func (e *EngineInstance) enforceTransactionCommitLimit(t *sladder.Transaction, rcs []*sladder.KVTransactionRecord) (bool, error) {
	for _, rc := range rcs {
		if e.cluster.Self() != rc.Node && (rc.New || rc.Delete) && !rc.Node.Anonymous() {
			return false, ErrTransactionCommitViolation
		}
	}
	return true, nil
}

type wrapVersionKVValidator struct {
	log sladder.Logger
	ov  sladder.KVValidator
}

// WrapVersionKVValidator wraps existing validator to make versioned data model.
func WrapVersionKVValidator(v sladder.KVValidator, log sladder.Logger) sladder.KVValidator {
	if log == nil {
		log = sladder.DefaultLogger
	}
	return wrapVersionKVValidator{
		ov:  v,
		log: log,
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

type wrapVersionKVTxn struct {
	origin wrapVersionKV
	o      sladder.KVTransaction
}

// After returns new value wrapped with version.
func (t *wrapVersionKVTxn) After() (changed bool, new string) {
	if changed, new = t.o.After(); changed {
		var err error

		if new, err = (&wrapVersionKV{Value: new, Version: t.origin.Version + 1}).Encode(); err != nil {
			panic(err)
		}
	}
	return
}

// Before return origin raw value wrapped with version.
func (t *wrapVersionKVTxn) Before() string {
	ori, err := t.origin.Encode()
	if err != nil {
		panic(err)
	}
	return ori
}

func (t *wrapVersionKVTxn) KVTransaction() sladder.KVTransaction { return t.o }

func (v wrapVersionKVValidator) Sync(lr *sladder.KeyValue, rr *sladder.KeyValue) (changed bool, err error) {
	// local
	if lr == nil {
		return false, nil
	}
	or := wrapVersionKV{}
	if err := or.Decode(lr.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Sync() got invalid local value. err = \"%v\"", err)
		return false, err
	}

	// remote
	if rr == nil { // a deletion
		return v.ov.Sync(&sladder.KeyValue{
			Key: lr.Key, Value: or.Value,
		}, nil)
	}
	cr := wrapVersionKV{}
	if err := cr.Decode(rr.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Sync() got invalid remote value. err = \"%v\"", err)
		return false, err
	}

	// normal sync.
	obuf, cbuf := &sladder.KeyValue{Key: lr.Key, Value: or.Value}, &sladder.KeyValue{Key: rr.Key, Value: cr.Value}
	if changed, err = v.ov.Sync(obuf, cbuf); err != nil {
		return false, err
	}
	if or.Value != obuf.Value {
		changed = true
	}
	if changed {
		// save changes.
		or.Value, or.Version = obuf.Value, cr.Version
		new, ierr := or.Encode()
		if ierr != nil {
			return false, ierr
		}
		lr.Value = new
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

	wrap := &wrapTxn.origin
	if err := wrap.Decode(x.Value); err != nil {
		v.log.Warnf("wrapVersionKV.Txn() decoding got invalid value. err = \"%v\"", err)
		return nil, err
	}

	if txn, err = v.ov.Txn(sladder.KeyValue{
		Key: x.Key, Value: wrap.Value,
	}); err != nil {
		return nil, err
	}
	wrapTxn.o = txn

	return wrapTxn, nil
}
