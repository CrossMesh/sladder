package sladder

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/sunmxt/sladder/proto"
)

var (
	ErrValidatorMissing    = errors.New("missing validator")
	ErrRejectedByValidator = errors.New("operation rejected by validator")
	ErrInvalidKeyValue     = errors.New("invalid key value pair")
)

// Node represents members of cluster.
type Node struct {
	names []string

	lock    sync.RWMutex
	kvs     map[string]*KeyValueEntry
	cluster *Cluster
}

func newNode(cluster *Cluster) *Node {
	return &Node{
		cluster: cluster,
		kvs:     make(map[string]*KeyValueEntry),
	}
}

// Names returns name set.
func (n *Node) Names() (names []string) {
	n.lock.Lock()
	defer n.lock.Unlock()

	names = make([]string, len(n.names))
	copy(names, n.names)
	return
}

func (n *Node) assignNames(names []string, sorted bool) {
	if !sorted {
		sort.Strings(names)
	}
	n.names = names
}

// Keys selects keys.
func (n *Node) Keys(keys ...string) *OperationContext {
	return (&OperationContext{cluster: n.cluster}).Keys(keys...).Nodes(n)
}

// KeyValueEntries return array existing entries.
func (n *Node) KeyValueEntries(clone bool) (entries []*KeyValue) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.keyValueEntries(clone)
}

func (n *Node) keyValueEntries(clone bool) (entries []*KeyValue) {
	for _, entry := range n.kvs {
		kv := &entry.KeyValue
		if clone {
			kv = kv.Clone()
		}
		entries = append(entries, kv)
	}
	return
}

// Delete remove KeyValue.
func (n *Node) Delete(key string) (bool, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.delete(key)
}

func (n *Node) delete(key string) (bool, error) {
	entry := n.get(key)
	if entry == nil {
		return false, nil
	}
	accepted, err := entry.validator.Sync(entry, nil)
	if err != nil {
		return false, err
	}
	if !accepted {
		return false, ErrRejectedByValidator
	}

	delete(n.kvs, key)
	n.cluster.emitKeyDeletion(n, entry.Key, entry.Value, n.keyValueEntries(true))
	return true, nil
}

// Set sets KeyValue.
func (n *Node) Set(key, value string) error {
	var entry *KeyValueEntry
	var validator KVValidator

	n.lock.Lock()

	entry, exists := n.kvs[key]
	for !exists || entry == nil {
		// lock order should be preserved to avoid deadlock.
		// that is: acquire cluster lock, then acquire node lock.
		n.lock.Unlock()
		n.cluster.lock.RLock()
		validator, exists = n.cluster.validators[key]
		n.cluster.lock.RUnlock()
		if !exists {
			return ErrValidatorMissing
		}
		// new KV.
		newEntry := &KeyValueEntry{
			KeyValue: KeyValue{
				Key:   key,
				Value: value,
			},
			validator: validator,
		}
		if !validator.Validate(newEntry.KeyValue) {
			return ErrInvalidKeyValue
		}

		n.lock.Lock()
		if entry, exists = n.kvs[key]; exists {
			continue
		}
		n.kvs[key] = newEntry
		n.cluster.emitKeyInsertion(n, newEntry.Key, newEntry.Value, n.keyValueEntries(true))
		n.lock.Unlock()
		return nil
	}

	defer n.lock.Unlock()

	// modify existing entry.
	if !entry.validator.Validate(KeyValue{
		Key:   key,
		Value: value,
	}) {
		return ErrInvalidKeyValue
	}
	origin := entry.Value
	entry.Value = value
	entry.Key = key
	n.cluster.emitKeyChange(n, entry.Key, origin, entry.Value, n.keyValueEntries(true))

	return nil
}

func (n *Node) protobufSnapshot(message *proto.Node) {
	message.Kvs = make([]*proto.Node_KeyValue, 0, len(n.kvs))
	for key, entry := range n.kvs {
		entry.Key = key
		if entry.flags&LocalEntry != 0 { // local entry.
			continue
		}
		message.Kvs = append(message.Kvs, &proto.Node_KeyValue{
			Key:   entry.Key,
			Value: entry.Value,
		})
	}
}

// PrintableName returns node name string for print.
func (n *Node) PrintableName() string {
	if names := n.names; len(names) == 0 {
		return "_"
	} else if len(names) == 1 {
		return names[0]
	} else {
		return fmt.Sprintf("%v", names)
	}
}

// ProtobufSnapshot creates a node snapshot to protobuf message.
func (n *Node) ProtobufSnapshot(message *proto.Node) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.protobufSnapshot(message)
}

func (n *Node) get(key string) (entry *KeyValueEntry) {
	entry, _ = n.kvs[key]
	return
}

func (n *Node) replaceValidatorForce(key string, validator KVValidator) {
	n.lock.Lock()
	defer n.lock.Unlock()

	entry, exists := n.kvs[key]
	if !exists {
		return
	}

	// ensure that existing value is valid for new validator.
	if !validator.Validate(entry.KeyValue) {
		// drop entry in case of incompatiable validator.
		delete(n.kvs, key)
		n.cluster.emitKeyDeletion(n, entry.Key, entry.Value, n.keyValueEntries(true))
	} else {
		entry.validator = validator // replace.
	}

	return
}
