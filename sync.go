package sladder

import (
	"github.com/sunmxt/sladder/proto"
)

func (c *Cluster) resolveNodeNameFromProtobuf(entries []*proto.Node_KeyValue) []string {
	watchKeySet := make(map[string]*KeyValue)
	for _, key := range c.resolver.Keys() {
		watchKeySet[key] = nil
	}

	for _, entry := range entries {
		kv, watched := watchKeySet[entry.Key]
		if !watched {
			continue
		}
		if kv == nil {
			kv = &KeyValue{
				Key:   entry.Key,
				Value: entry.Value,
			}
		}
	}

	watchEntries := make([]*KeyValue, 0, len(watchKeySet))
	for _, kv := range watchKeySet {
		if kv == nil {
			continue
		}
		watchEntries = append(watchEntries, kv)
	}

	names, err := c.resolver.Resolve(watchEntries...)
	if err != nil {
		c.log.Fatal("failed to resolve node name from protobuf, got " + err.Error())
		return nil
	}

	return names
}

// SyncFromProtobufSnapshot sync cluster states by refering to protobuf snapshot.
func (c *Cluster) SyncFromProtobufSnapshot(s *proto.Cluster, autoNewNode bool) {
	if s == nil {
		return
	}

	// sync to existings
	for _, msg := range s.Nodes {
		names := c.resolveNodeNameFromProtobuf(msg.Kvs)
		node := c.mostPossibleNode(names)
		if node == nil {
			if !autoNewNode {
				continue
			}
			newNode, err := c.NewNode()
			if err != nil {
				c.log.Fatal("failed to create new node to sync, got ", err.Error())
				continue
			}
			node = newNode
		}
		node.SyncFromProtobufSnapshot(msg)
	}
}

// SyncFromProtobufSnapshot sync node states by refering to protobuf snapshot.
func (n *Node) SyncFromProtobufSnapshot(s *proto.Node) {
	if s == nil {
		return
	}

	var (
		validator KVValidator
		accepted  bool
	)
	existingKeySet := make(map[string]struct{}, len(s.Kvs))

	syncEntry := func(entry *KeyValueEntry, msgKV *proto.Node_KeyValue) bool {
		accepted, err := entry.validator.Sync(entry, &KeyValue{
			Key:   msgKV.Key,
			Value: msgKV.Value,
		})
		if err != nil {
			n.cluster.log.Fatalf("validator failure: %v {key = %v}", err, msgKV.Key)
			return false
		}
		return accepted
	}

	for _, msg := range s.Kvs {
		key := msg.Key

		entry, exists := n.kvs[key]
		if !exists { // new key-value
			if validator, _ = n.cluster.validators[key]; validator == nil {
				// not accaptable for missing validator.
				n.cluster.log.Warnf("missing key-value validator for new remote entry {nodeID = %v, key = %v}", n.PrintableName(), key)
			}
			entry = &KeyValueEntry{
				validator: validator,
				KeyValue: KeyValue{
					Key: key,
				},
			}
			if accepted = syncEntry(entry, msg); accepted {
				// save accepted entry.
				n.kvs[key] = entry
				n.cluster.emitKeyInsertion(n, entry.Key, entry.Value)
			}
		} else {
			// existing one.
			// sync to storage entry.
			origin := entry.Value
			if accepted = syncEntry(entry, msg); accepted {
				n.cluster.emitKeyChange(n, entry.Key, origin, entry.Value)
			}
		}
		if accepted {
			existingKeySet[key] = struct{}{}
		}
	}

	// delete old entry.
	for key, entry := range n.kvs {
		if _, exist := existingKeySet[entry.Key]; !exist && syncEntry(entry, nil) {
			delete(n.kvs, key)
			n.cluster.emitKeyDeletion(n, entry.Key, entry.Value)
		}
	}
}
