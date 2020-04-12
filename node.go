package sladder

import (
	"errors"
	"fmt"
	"sync"

	"github.com/sunmxt/sladder/proto"
)

var (
	ErrCoordinatorMissing = errors.New("missing coordinator")
	ErrInvalidKeyValue    = errors.New("invalid key value pair")
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

// Set sets KeyValue.
func (n *Node) Set(key, value string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	entry, exists := n.kvs[key]
	if !exists {
		// new KV.
		n.cluster.lock.RLock()
		coordinator, exists := n.cluster.coordinators[key]
		n.cluster.lock.RUnlock()

		if !exists {
			return ErrCoordinatorMissing
		}
		entry = &KeyValueEntry{
			KeyValue: KeyValue{
				Key:   key,
				Value: value,
			},
			coordinator: coordinator,
		}
		if !coordinator.Validate(entry.KeyValue) {
			return ErrInvalidKeyValue
		}
		n.kvs[key] = entry
		return nil
	}

	// modify existing entry.
	if !entry.coordinator.Validate(KeyValue{
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
		coordinator KVCoordinator
		accepted    bool
		err         error
	)

	for _, msg := range message.Kvs {
		key := msg.Key

		entry, exists := n.kvs[key]
		if !exists { // new key-value
			if coordinator, _ = n.cluster.coordinators[key]; coordinator == nil {
				// not acceptable for missing coordinator.
				n.cluster.log.Warnf("missing key-value coordinator for new entry {nodeID = %v, key = %v}", n.PrintableName(), key)
			}
			entry = &KeyValueEntry{
				coordinator: coordinator,
				KeyValue: KeyValue{
					Key: key,
				},
			}

			// sync to storage entry.
			if accepted, err = coordinator.Sync(entry, &KeyValue{
				Key:   key,
				Value: msg.Value,
			}); err != nil {
				n.cluster.log.Fatal("[coordinator] ", err)
				continue
			}
			if accepted {
				// save accepted entry.
				n.kvs[key] = entry
			}

		} else {
			// existing one.
			coordinator = entry.coordinator
			// sync to storage entry.
			if accepted, err = coordinator.Sync(entry, &KeyValue{
				Key:   key,
				Value: msg.Value,
			}); err != nil {
				n.cluster.log.Fatal("[coordinator] ", err)
				continue
			}
		}

		if accepted {
			existingKeySet[key] = struct{}{}
		}
	}

	// delete missing key.
	for key, entry := range n.kvs {
		entry.Key, coordinator = key, entry.coordinator
		if accepted, err = coordinator.Sync(entry, nil); err != nil {
			n.cluster.log.Fatal("[coordinator] ", err)
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

func (n *Node) replaceCoordinatorForce(key string, coordinator KVCoordinator) {
	n.lock.Lock()
	defer n.lock.Unlock()

	entry, exists := n.kvs[key]
	if !exists {
		return
	}

	// ensure that existing value is valid for new coordinator.
	if !coordinator.Validate(entry.KeyValue) {
		// drop entry in case of incompatiable coordinator.
		delete(n.kvs, key)
	} else {
		entry.coordinator = coordinator // replace.
	}

	return
}
