package sladder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestKVValidatorWrapper struct {
	Prefix string
	ov     KVValidator
}

func (w *TestKVValidatorWrapper) Sync(l, r *KeyValue) (bool, error) {
	if l == nil {
		return false, nil
	}
	prefix := w.Prefix
	if len(l.Value) < len(prefix) || l.Value[:len(prefix)] != prefix {
		return false, errors.New("invalid wrapped prefix")
	}
	if r == nil {
		return w.ov.Sync(l, nil)
	}
	return w.ov.Sync(l, &KeyValue{
		Key: l.Key, Value: l.Value[len(prefix):],
	})
}

func (w *TestKVValidatorWrapper) Validate(kv KeyValue) bool {
	prefix := w.Prefix
	if len(kv.Value) < len(prefix) || kv.Value[:len(prefix)] != prefix {
		return true
	}
	return w.ov.Validate(KeyValue{
		Key: kv.Key, Value: kv.Value[len(prefix):],
	})
}

func WrapTestKVTransaction(prefix string, ov KVValidator) KVValidator {
	return &TestKVValidatorWrapper{Prefix: prefix, ov: ov}
}

type TestKVValidatorWrapperTxn struct {
	prefix string

	txn KVTransaction
}

func (w *TestKVValidatorWrapper) Txn(kv KeyValue) (KVTransaction, error) {
	prefix := w.Prefix
	if len(kv.Value) < len(prefix) || kv.Value[:len(prefix)] != prefix {
		return nil, errors.New("invalid wrapped prefix")
	}
	inner, err := w.ov.Txn(KeyValue{
		Key: kv.Key, Value: kv.Value[len(prefix):],
	})
	if err != nil {
		return nil, err
	}
	return &TestKVValidatorWrapperTxn{txn: inner, prefix: prefix}, nil
}

func (v *TestKVValidatorWrapperTxn) Updated() bool  { return v.txn.Updated() }
func (v *TestKVValidatorWrapperTxn) After() string  { return v.prefix + v.txn.After() }
func (v *TestKVValidatorWrapperTxn) Before() string { return v.prefix + v.txn.Before() }
func (v *TestKVValidatorWrapperTxn) SetRawValue(x string) error {
	prefix := v.prefix
	if x[:len(prefix)] != prefix {
		return errors.New("invalid wrapped prefix")
	}
	return v.SetRawValue(x)
}
func (v *TestKVValidatorWrapperTxn) KVTransaction() KVTransaction { return v.txn }

func TestKeyValue(t *testing.T) {
	kv := &KeyValue{
		Key:   "k",
		Value: "v",
	}
	c := kv.Clone()
	assert.Equal(t, kv.Key, c.Key)
	assert.Equal(t, kv.Value, c.Value)
}
