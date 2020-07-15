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
			watchKeySet[entry.Key] = &KeyValue{
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
func (c *Cluster) SyncFromProtobufSnapshot(s *proto.Cluster, autoNewNode bool,
	validate func(*Node, []*proto.Node_KeyValue) bool,
	validateEntry func(*Node, *KeyValue, *proto.Node_KeyValue) bool,
) {
	if s == nil {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// sync to existings
	for _, msg := range s.Nodes {
		names := c.resolveNodeNameFromProtobuf(msg.Kvs)
		if len(names) < 1 { // skip syncing anonymous empty node.
			continue
		}
		node := c.mostPossibleNode(names)
		if validate != nil && !validate(node, msg.Kvs) {
			continue
		}
		if node == nil {
			if !autoNewNode {
				continue
			}
			newNode, err := c.newNode()
			if err != nil {
				c.log.Fatal("failed to create new node to sync, got ", err.Error())
				continue
			}
			if len(names) > 0 { // named node.
				// update node names now to ensure consistency between node names and entries.
				c.updateNodeName(newNode, names)
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

	syncEntry := func(entry *KeyValueEntry, msgKV *proto.Node_KeyValue) (accepted bool) {
		var err error

		if msgKV == nil {
			accepted, err = entry.validator.Sync(&entry.KeyValue, nil)
		} else {
			accepted, err = entry.validator.Sync(&entry.KeyValue, &KeyValue{
				Key:   msgKV.Key,
				Value: msgKV.Value,
			})
		}
		if err != nil {
			n.cluster.log.Fatalf("validator failure: %v {key = %v}", err, entry.Key)
			return false
		}
		return accepted
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	for _, msg := range s.Kvs {
		key := msg.Key

		entry, exists := n.kvs[key]
		if !exists { // new key-value
			if validator, _ = n.cluster.validators[key]; validator == nil {
				// not accaptable for missing validator.
				n.cluster.log.Warnf("missing key-value validator for new remote entry {nodeID = %v, key = %v}", n.PrintableName(), key)
				continue
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
				n.cluster.emitKeyInsertion(n, entry.Key, entry.Value, n.keyValueEntries(true))
			}
		} else {
			// existing one.
			// sync to storage entry.
			origin := entry.Value
			if accepted = syncEntry(entry, msg); accepted {
				n.cluster.emitKeyChange(n, entry.Key, origin, entry.Value, n.keyValueEntries(true))
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
			n.cluster.emitKeyDeletion(n, entry.Key, entry.Value, n.keyValueEntries(true))
		}
	}
}
