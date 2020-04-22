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
		return nil
	}

	// modify existing entry.
	if !entry.validator.Validate(KeyValue{
		Key:   key,
		Value: value,
	}) {
		return ErrInvalidKeyValue
	}
	entry.Value = value
	entry.Key = key

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

func (n *Node) synchrionizeFromProtobufSnapshot(message *proto.Node) {
	if message == nil {
		return
	}

	existingKeySet := make(map[string]struct{}, len(message.Kvs))
	var (
		validator KVValidator
		accepted  bool
		err       error
	)

	for _, msg := range message.Kvs {
		key := msg.Key

		entry, exists := n.kvs[key]
		if !exists { // new key-value
			if validator, _ = n.cluster.validators[key]; validator == nil {
				// not acceptable for missing validator.
				n.cluster.log.Warnf("missing key-value validator for new entry {nodeID = %v, key = %v}", n.PrintableName(), key)
			}
			entry = &KeyValueEntry{
				validator: validator,
				KeyValue: KeyValue{
					Key: key,
				},
			}

			// sync to storage entry.
			if accepted, err = validator.Sync(entry, &KeyValue{
				Key:   key,
				Value: msg.Value,
			}); err != nil {
				n.cluster.log.Fatal("[validator] ", err)
				continue
			}
			if accepted {
				// save accepted entry.
				n.kvs[key] = entry
			}

		} else {
			// existing one.
			validator = entry.validator
			// sync to storage entry.
			if accepted, err = validator.Sync(entry, &KeyValue{
				Key:   key,
				Value: msg.Value,
			}); err != nil {
				n.cluster.log.Fatal("[validator] ", err)
				continue
			}
		}

		if accepted {
			existingKeySet[key] = struct{}{}
		}
	}

	// delete missing key.
	for key, entry := range n.kvs {
		entry.Key, validator = key, entry.validator
		if accepted, err = validator.Sync(entry, nil); err != nil {
			n.cluster.log.Fatal("[validator] ", err)
			continue
		}
		if accepted {
			delete(n.kvs, key)
		}
	}

	return
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
	} else {
		entry.validator = validator // replace.
	}

	return
}
