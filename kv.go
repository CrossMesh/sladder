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
	After() (bool, string) // After returns new value.
	Before() string        // Before return origin raw value.
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
