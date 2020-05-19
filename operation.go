package sladder

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidTransactionHandler = errors.New("invalid transaction handler")
	ErrTooManyNodes              = errors.New("too many nodes selected")
	ErrRejectedByCoordinator     = errors.New("coordinator rejects the transaction")
	ErrInvalidNode               = errors.New("invalid node")
)

// KVTransactionRecord contains metadata of a kv txn.
type KVTransactionRecord struct {
	LC   uint32 // local logic clock.
	Node *Node
	Key  string
	Txn  KVTransaction
}

// TxnCoordinator traces transaction.
type TxnCoordinator interface {
	TransactionStart(*Transaction) (bool, error)
	TransactionBeginKV(*Transaction, *Node, string) (*KeyValue, error)
	TransactionCommit(*Transaction, []*KVTransactionRecord) (bool, error)
	TransactionRollback(*Transaction) error
}

type txnKeyRef struct {
	node *Node
	key  string
}

type txnTrace struct {
	txn KVTransaction
	lc  uint32
}

// Transaction contains read-write operation on nodes.
type Transaction struct {
	lc          uint32
	Cluster     *Cluster
	coordinator TxnCoordinator

	lock           sync.RWMutex
	txns           map[txnKeyRef]*txnTrace
	relatedNodes   map[*Node]struct{}
	nodeSnapshoted bool
}

func newTransaction(c *Cluster) *Transaction {
	return &Transaction{
		Cluster: c,
		lc:      0,
		txns:    make(map[txnKeyRef]*txnTrace),
	}
}

// Txn executes new transaction.
func (c *Cluster) Txn(do func(*Transaction) bool) (err error) {
	t, commit := newTransaction(c), true
	t.coordinator, _ = c.engine.(TxnCoordinator)

	c.lock.RLock()
	defer c.lock.RUnlock()

	// before transaction.
	if t.coordinator != nil {
		if commit, err = t.coordinator.TransactionStart(t); err != nil {
			return err
		}
		if !commit {
			return ErrRejectedByCoordinator
		}
	}
	// do transaction.
	if commit = do(t); !commit {
		if t.coordinator != nil {
			err = t.coordinator.TransactionRollback(t)
		}
		t.cancel()
		return
	}
	// start commit
	if t.coordinator != nil {
		txns := make([]*KVTransactionRecord, 0, len(t.txns))
		for ref, trace := range t.txns {
			txns = append(txns, &KVTransactionRecord{
				Node: ref.node,
				Key:  ref.key,
				Txn:  trace.txn,
				LC:   trace.lc,
			})
		}
		// sort by local lc.
		sort.Slice(txns, func(i, j int) bool {
			return txns[i].LC < txns[j].LC
		})
		if commit, err = t.coordinator.TransactionCommit(t, txns); err != nil {
			t.cancel()
			return err
		}
	}
	// save to local.
	if commit {
		t.commit()
	} else {
		t.cancel()
		return ErrRejectedByCoordinator
	}

	return nil
}

// RangeNode iterates over nodes.
func (t *Transaction) RangeNode(visit func(*Node) bool, excludeSelf, excludeEmpty bool) {
	for {
		snapshoted := t.nodeSnapshoted
		if snapshoted {
			t.Cluster.rangeNodes(visit, excludeSelf, excludeEmpty)
			break
		}
		t.lock.Lock()
		if !t.nodeSnapshoted {
			t.Cluster.lock.RLock()
			t.nodeSnapshoted = true
		}
		t.lock.Unlock()
	}
}

func (t *Transaction) getCacheKV(ref txnKeyRef, lc uint32) *txnTrace {
	if trace := t.txns[ref]; trace != nil {
		// update logic clock.
		for {
			lastLC := trace.lc
			if lastLC >= lc || atomic.CompareAndSwapUint32(&trace.lc, lastLC, lc) {
				break
			}
		}
		return trace
	}
	return nil
}

func (t *Transaction) cleanLocks() {
	// unlock all related nodes.
	for node := range t.relatedNodes {
		node.lock.Unlock()
	}
	if t.nodeSnapshoted {
		// unlock cluster read-lock.
		t.Cluster.lock.RUnlock()
	}
}

func (t *Transaction) commit() {
	for ref, trace := range t.txns {
		updated, newValue := trace.txn.After()
		entry, exists := ref.node.kvs[ref.key]
		if exists && entry != nil {
			if updated {
				// updated
				origin := entry.Value
				entry.Value = newValue
				t.Cluster.emitKeyChange(ref.node, entry.Key, origin, newValue)
			}
			entry.lock.Unlock()
		} else if updated {
			// new
			entry = &KeyValueEntry{
				KeyValue: KeyValue{
					Key:   ref.key,
					Value: newValue,
				},
			}
			ref.node.kvs[ref.key] = entry
			t.Cluster.emitKeyInsertion(ref.node, entry.Key, newValue)
		}
	}
	t.cleanLocks()
}

func (t *Transaction) cancel() {
	for ref := range t.txns {
		entry, exists := ref.node.kvs[ref.key]
		if exists && entry != nil {
			entry.lock.Unlock()
		}
	}
	t.cleanLocks()
}

// KV start kv transaction on node.
func (t *Transaction) KV(n *Node, key string) (txn KVTransaction, err error) {
	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.
	ref := txnKeyRef{node: n, key: key}

	t.lock.RLock()
	trace := t.getCacheKV(ref, lc)
	t.lock.RUnlock()
	if trace != nil {
		return trace.txn, nil
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	if trace = t.getCacheKV(ref, lc); trace != nil {
		return trace.txn, nil
	}

	trace = &txnTrace{lc: lc}

	validator, hasValidator := t.Cluster.validators[key]
	if !hasValidator {
		return nil, ErrValidatorMissing
	}
	if n.cluster != t.Cluster {
		return nil, ErrInvalidNode
	}

	if t.relatedNodes == nil {
		t.relatedNodes = make(map[*Node]struct{})
	}
	if _, locked := t.relatedNodes[n]; !locked {
		n.lock.Lock()
		t.relatedNodes[n] = struct{}{}
	}

	var existing *KeyValueEntry
	var snap *KeyValue
	if t.coordinator != nil {
		latestSnap, err := t.coordinator.TransactionBeginKV(t, n, key)
		if err != nil {
			return nil, err
		}
		// perfer coordinator provided snapshot.
		if latestSnap != nil {
			snap = latestSnap
		}
	}
	if snap == nil {
		// using local snapshot
		if e, exists := n.kvs[key]; !exists || e == nil {
			// new empty entry if not exist.
			snap = &KeyValue{Key: key}
		} else {
			// lock this entry.
			e.lock.Lock()
			existing = e
			snap = &e.KeyValue
		}
	}
	// create entry transaction
	if txn, err = validator.Txn(*snap); err != nil {
		if existing != nil {
			existing.lock.Unlock()
		}
		t.Cluster.log.Fatalf("validator of key \"%v\" failed to create transaction: " + err.Error())
	}
	// save
	trace.txn = txn
	t.txns[ref] = trace

	return
}

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
	if len(nc.nodes) > 0 || nc.nodes == nil {
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

// Watch watches changes.
func (c *OperationContext) Watch(handler WatchEventHandler) *WatchEventContext {
	if handler == nil {
		// ignore dummy handler.
		return nil
	}

	return c.cluster.eventRegistry.watchKV(c.clone(), handler)
}
