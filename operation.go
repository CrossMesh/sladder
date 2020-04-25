package sladder

import (
	"errors"
	"reflect"
)

var (
	ErrInvalidTransactionHandler = errors.New("invalid transaction handler")
	ErrTooManyNodes              = errors.New("too many nodes selected")
)

// OperationContext traces operation scope.
type OperationContext struct {
	keys      []string
	nodeNames []string
	nodes     map[*Node]struct{}

	cluster *Cluster

	Error error
}

func (c *OperationContext) clone() *OperationContext {
	new := &OperationContext{cluster: c.cluster}

	if len(c.keys) > 0 {
		new.keys = append(new.keys, c.keys...)
	}
	if len(c.nodeNames) > 0 {
		new.nodeNames = append(new.nodeNames, c.nodeNames...)
	}
	if len(c.nodes) > 0 {
		new.nodes = make(map[*Node]struct{})
		for node := range c.nodes {
			new.nodes[node] = struct{}{}
		}
	}

	return new
}

// Nodes filters nodes.
func (c *OperationContext) Nodes(nodes ...interface{}) *OperationContext {
	nc := c.clone()
	if len(nc.nodes) > 0 {
		nc.nodes = make(map[*Node]struct{})
	}
	if len(nc.nodeNames) > 0 {
		nc.nodeNames = nc.nodeNames[:0]
	}
	for _, node := range nodes {
		switch v := node.(type) {
		case string:
			nc.nodeNames = append(nc.nodeNames, v)
		case *Node:
			if v.cluster == nil || v.cluster != nc.cluster {
				continue
			}
			nc.nodes[v] = struct{}{}
		default:
		}
	}
	return nc
}

// Keys filters keys.
func (c *OperationContext) Keys(keys ...string) *OperationContext {
	nc := c.clone()
	if len(nc.keys) > 0 {
		nc.keys = nc.keys[:0]
	}
	nc.keys = append(nc.keys, keys...)
	return nc
}

func (c *OperationContext) resolveNodes(maxNodes int) (nodes []*Node, err error) {
	fullfilled := func() bool { return maxNodes > 0 && len(nodes) >= maxNodes }

	nodeSet := make(map[*Node]struct{})

	for node := range c.nodes {
		if fullfilled() {
			return nil, ErrTooManyNodes
		}
		nodeSet[node] = struct{}{}
		nodes = append(nodes, node)
	}

	for _, name := range c.nodeNames {
		node := c.cluster.getNode(name)
		if node == nil {
			continue
		}
		if fullfilled() {
			return nil, ErrTooManyNodes
		}
		if _, exists := nodeSet[node]; exists {
			continue
		}
		nodeSet[node] = struct{}{}
		nodes = append(nodes, node)
	}

	return
}

// Txn executes a transaction.
func (c *OperationContext) Txn(proc interface{}) *OperationContext {
	withError := func(err error) *OperationContext {
		nc := c.clone()
		nc.Error = ErrInvalidTransactionHandler
		return nc
	}

	if len(c.keys) < 1 {
		// ignore dummy txn.
		return withError(nil)
	}

	// lock cluster to prevent from node changing.
	c.cluster.lock.RLock()
	defer c.cluster.lock.RUnlock()

	// basic check proc type.
	ty := reflect.TypeOf(proc)
	if ty.Kind() != reflect.Func || // handler should be func
		ty.NumOut() != 2 || ty.Out(0).Kind() != reflect.Bool || ty.Out(1) != reflect.TypeOf(error(nil)) || // return bool, error
		ty.NumIn() != len(c.keys) {
		return withError(ErrInvalidTransactionHandler)
	}

	// determine node.
	nodes, err := c.resolveNodes(1)
	if err != nil {
		return withError(err)
	}
	if len(nodes) < 1 {
		// ignore dummy txn.
		return withError(nil)
	}
	node := nodes[0]
	node.lock.Lock()
	defer node.lock.Unlock()

	// prepare transaction.
	validators := make([]KVValidator, len(c.keys))
	for idx, key := range c.keys {
		validator, hasValidator := c.cluster.validators[key]
		if !hasValidator {
			return withError(ErrValidatorMissing)
		}
		validators[idx] = validator
	}
	kvTxns := make([]KVTransaction, len(c.keys))
	for idx, key := range c.keys {
		var kv *KeyValue

		if entry, exists := node.kvs[key]; !exists || entry == nil {
			// new empty entry if not exists.
			kv = &KeyValue{Key: key}
		} else {
			kv = &entry.KeyValue

			// lock kv entry.
			entry.lock.Lock()
			defer entry.lock.Unlock()
		}

		txn, err := validators[idx].Txn(*kv)
		if err != nil {
			c.cluster.log.Fatalf("validator of key \"%v\" failed to create transaction: " + err.Error())
			return withError(err)
		}
		kvTxns[idx] = txn
	}

	// invoke transaction.
	// NOTE: heavy performance penalty for reflect.Call.
	callValues := make([]reflect.Value, len(c.keys))
	for idx, txn := range kvTxns {
		callValues[idx] = reflect.ValueOf(txn)
	}
	txn := reflect.ValueOf(proc)
	resultValues := txn.Call(callValues)

	if err := resultValues[1].Interface().(error); err != nil {
		// rollback for error.
		return withError(err)
	}
	if rollback := !resultValues[0].Bool(); !rollback {
		// rollback. write nothing.
		return withError(nil)
	}

	// write values.
	for idx, key := range c.keys {
		txn := kvTxns[idx]
		updated, newValue := txn.After()
		entry, exists := node.kvs[key]

		if updated {
			if exists && entry != nil { // updated
				origin := entry.Value
				entry.Value = newValue
				node.cluster.emitKeyChange(node, entry.Key, origin, newValue)
			} else {
				node.kvs[key] = &KeyValueEntry{
					KeyValue: KeyValue{
						Key:   key,
						Value: newValue,
					},
				}
				node.cluster.emitKeyInsertion(node, entry.Key, newValue)
			}
		}
	}

	return withError(nil)
}

// Watch watches changes.
func (c *OperationContext) Watch(handler WatchEventHandler) *OperationContext {
	if handler == nil {
		// ignore dummy handler.
		return c
	}

	c.cluster.eventRegistry.watchKV(c.clone(), handler)

	return c
}
