package sladder

import (
	"sync"
)

// KVValidator guards consistency of KeyValue.
type KVValidator interface {
	// Sync validates given KeyValue and update local key-value entry with it.
	Sync(*KeyValue, *KeyValue) (bool, error)

	// Validate validates given KeyValue
	Validate(KeyValue) bool

	// Txn begins an transaction.
	Txn(KeyValue) (KVTransaction, error)
}

// KVMergingProperties contains extra properties for merging.
type KVMergingProperties interface {
	// Concurrent indicates that the new conflicts with the existing one.
	Concurrent() bool

	Get(string) interface{}
}

// KVExtendedSyncer merges remote entry with extended merging properties.
type KVExtendedSyncer interface {
	SyncEx(*KeyValue, *KeyValue, KVMergingProperties) (bool, error)
}

// KVTransaction implements atomic operation.
type KVTransaction interface {
	// TODO(xutao): seperate 'updated' and 'newValue'
	Updated() bool            // Updated return whether value is updated.
	After() string            // After returns new value.
	Before() string           // Before return origin raw value.
	SetRawValue(string) error // SetRawValue set new raw value.
}

// KVTransactionWrapper wraps KVTransaction.
type KVTransactionWrapper interface {
	KVTransaction
	KVTransaction() KVTransaction
}

// KeyValueEntry holds KeyValue.
type KeyValueEntry struct {
	KeyValue

	flags uint32

	validator KVValidator
	lock      sync.RWMutex
}

const (
	// LocalEntry will not be synced to remote.
	LocalEntry = uint32(0x1)
)

// KeyValue stores one metadata key of the node.
type KeyValue struct {
	Key   string
	Value string
}

// Clone create a deepcopy of entries.
func (e *KeyValue) Clone() *KeyValue {
	return &KeyValue{
		Key:   e.Key,
		Value: e.Value,
	}
}

// StringValidator implements basic string KV.
type StringValidator struct{}

// SyncEx syncs string KV refering to properties.
func (v StringValidator) SyncEx(lr, rr *KeyValue, props KVMergingProperties) (bool, error) {
	if props.Concurrent() && lr != nil && rr != nil {
		if rr.Value > lr.Value {
			lr.Value = rr.Value
		}
	}
	return v.Sync(lr, rr)
}

// Sync syncs string KV.
func (v StringValidator) Sync(lr, rr *KeyValue) (bool, error) {
	if lr == nil {
		return false, nil
	}
	if rr == nil {
		return true, nil
	}
	lr.Value = rr.Value
	return true, nil
}

// Validate validates string KV.
func (v StringValidator) Validate(KeyValue) bool { return true }

// StringTxn implements string KV transaction.
type StringTxn struct {
	origin, new string
}

// Txn begins a KV transaction.
func (v StringValidator) Txn(x KeyValue) (KVTransaction, error) {
	return &StringTxn{origin: x.Value, new: x.Value}, nil
}

// After returns new string.
func (t *StringTxn) After() string { return t.new }

// Updated checks whether string updated.
func (t *StringTxn) Updated() bool { return t.origin != t.new }

// Before returns origin string.
func (t *StringTxn) Before() string { return t.origin }

// Get returns current string.
func (t *StringTxn) Get() string { return t.new }

// Set sets new string.
func (t *StringTxn) Set(x string) { t.new = x }

// SetRawValue sets new string.
func (t *StringTxn) SetRawValue(x string) error { t.Set(x); return nil }
