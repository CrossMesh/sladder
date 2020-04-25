package sladder

import (
	"errors"
	"fmt"
	"sync"

	"github.com/sunmxt/sladder/proto"
)

var (
	ErrValidatorMissing = errors.New("missing validator")
	ErrInvalidKeyValue  = errors.New("invalid key value pair")
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

// Keys selects keys.
func (n *Node) Keys(keys ...string) *OperationContext {
	return (&OperationContext{cluster: n.cluster}).Keys(keys...)
}

// KeyValueEntries return array existing entries.
func (n *Node) KeyValueEntries(clone bool) (entries []*KeyValue) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	for _, entry := range n.kvs {
		kv := &entry.KeyValue
		if clone {
			kv = kv.Clone()
		}
		entries = append(entries, kv)
	}

	return
}

// Set sets KeyValue.
func (n *Node) Set(key, value string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	entry, exists := n.kvs[key]
	if !exists {
		// new KV.
		n.cluster.lock.RLock()
		validator, exists := n.cluster.validators[key]
		n.cluster.lock.RUnlock()

		if !exists {
			return ErrValidatorMissing
		}
		entry = &KeyValueEntry{
			KeyValue: KeyValue{
				Key:   key,
				Value: value,
			},
			validator: validator,
		}
		if !validator.Validate(entry.KeyValue) {
			return ErrInvalidKeyValue
		}
		n.kvs[key] = entry
		n.cluster.emitKeyInsertion(n, entry.Key, entry.Value)
		return nil
	}

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
	n.cluster.emitKeyChange(n, entry.Key, origin, entry.Value)

	return nil
}

func (n *Node) protobufSnapshot(message *proto.Node) {
	message.Kvs = make([]*proto.Node_KeyValue, 0, len(n.kvs))
	for key, entry := range n.kvs {
		entry.Key = key
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
		n.cluster.emitKeyDeletion(n, entry.Key, entry.Value)
	} else {
		entry.validator = validator // replace.
	}

	return
}
