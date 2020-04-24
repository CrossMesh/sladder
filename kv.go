package sladder

import (
	"sync"
)

// KVValidator guards consistency of KeyValue.
type KVValidator interface {

	// Sync validates given KeyValue and update local key-value entry with it.
	Sync(*KeyValueEntry, *KeyValue) (bool, error)

	// Validate validates given KeyValue
	Validate(KeyValue) bool

	// Txn begins an transaction.
	Txn(KeyValue) (KVTransaction, error)
}

// KVTransaction implements atomic operation.
type KVTransaction interface {
	// After returns new value.
	After() (bool, string)
}

// KeyValueEntry holds KeyValue.
type KeyValueEntry struct {
	KeyValue

	flags uint32

	validator KVValidator
	lock      sync.RWMutex
}

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
