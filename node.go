package sladder

import (
	"fmt"
	"sync"

	"github.com/sunmxt/sladder/proto"
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
