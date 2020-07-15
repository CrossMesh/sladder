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
	ErrInvalidNode               = errors.New("invalid node")
)

// KVTransactionRecord contains metadata of a kv txn.
type KVTransactionRecord struct {
	LC          uint32 // local logic clock.
	Node        *Node
	Key         string
	Txn         KVTransaction
	Delete, New bool
}

// TxnStartCoordinator traces start of transaction
type TxnStartCoordinator interface {
	TransactionStart(*Transaction) (bool, error)
}

// TxnKVCoordinator traces Transaction.KV()
type TxnKVCoordinator interface {
	TransactionBeginKV(*Transaction, *Node, string) (*KeyValue, error)
}

// TxnCommitCoordinator traces transaction commit.
type TxnCommitCoordinator interface {
	TransactionCommit(*Transaction, []*KVTransactionRecord) (bool, error)
}

// TxnRollbackCoordinator traces transaction rollback.
type TxnRollbackCoordinator interface {
	TransactionRollback(*Transaction) error
}

type txnKeyRef struct {
	node *Node
	key  string
}

type txnLog struct {
	// nil KVTransaction stands for a deletion.
	txn       KVTransaction
	validator KVValidator
	lc        uint32
	new       bool
}

// Transaction contains read-write operation on nodes.
type Transaction struct {
	lc      uint32
	Cluster *Cluster

	lock           sync.RWMutex
	logs           map[txnKeyRef]*txnLog
	relatedNodes   map[*Node]struct{}
	nodeSnapshoted bool
}

func newTransaction(c *Cluster) *Transaction {
	return &Transaction{
		Cluster: c,
		lc:      0,
		logs:    make(map[txnKeyRef]*txnLog),
	}
}

// Txn executes new transaction.
func (c *Cluster) Txn(do func(*Transaction) bool) (err error) {
	t, commit := newTransaction(c), true

	c.lock.RLock()
	defer c.lock.RUnlock()

	// before transaction.
	if starter, _ := c.engine.(TxnStartCoordinator); starter != nil {
		if commit, err = starter.TransactionStart(t); err != nil {
			return err
		}
		if !commit {
			return ErrRejectedByCoordinator
		}
	}
	// do transaction.
	if commit = do(t); !commit {
		if rollbacker, _ := c.engine.(TxnRollbackCoordinator); rollbacker != nil {
			err = rollbacker.TransactionRollback(t)
		}
		t.cancel()
		return
	}
	// start commit
	if commiter, _ := c.engine.(TxnCommitCoordinator); commiter != nil {
		txns := make([]*KVTransactionRecord, 0, len(t.logs))
		for ref, log := range t.logs {
			rc := &KVTransactionRecord{
				Node: ref.node,
				Key:  ref.key,
				Txn:  log.txn,
				LC:   log.lc,
				New:  log.new,
			}
			if log.txn == nil {
				rc.Delete = true
			}
			txns = append(txns, rc)
		}
		// sort by local lc.
		sort.Slice(txns, func(i, j int) bool {
			return txns[i].LC < txns[j].LC
		})
		if commit, err = commiter.TransactionCommit(t, txns); err != nil {
			t.cancel()
			return err
		}
	}
	// save to local.
	if commit {
		t.commit()
	} else {
		t.cancel()
		return ErrRejectedByValidator
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

func (t *Transaction) getCacheKV(ref txnKeyRef, lc uint32) *txnLog {
	if trace := t.logs[ref]; trace != nil {
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
	// TODO(xutao): ensure consistency between entries and node names.
	removedEntries, removedRefs := make([]*KeyValueEntry, 0), make([]*txnKeyRef, 0)

	for ref, trace := range t.logs {
		entry, exists := ref.node.kvs[ref.key]
		if trace.txn != nil {
			updated, newValue := trace.txn.After()
			if exists && entry != nil { // exists.
				if updated {
					// updated
					origin := entry.Value
					entry.Value = newValue
					t.Cluster.emitKeyChange(ref.node, entry.Key, origin, newValue, ref.node.keyValueEntries(true))
				}
				entry.lock.Unlock()
			} else if updated {
				// new
				entry = &KeyValueEntry{
					KeyValue: KeyValue{
						Key:   ref.key,
						Value: newValue,
					},
					validator: trace.validator,
				}
				ref.node.kvs[ref.key] = entry
				t.Cluster.emitKeyInsertion(ref.node, entry.Key, newValue, ref.node.keyValueEntries(true))
			}
		} else if exists && entry != nil {
			delete(ref.node.kvs, ref.key) // remove
		}
	}

	for idx, entry := range removedEntries { // send delete events.
		t.Cluster.emitKeyDeletion(removedRefs[idx].node, entry.Key, entry.Value, removedRefs[idx].node.keyValueEntries(true))
	}
	t.cleanLocks()
}

func (t *Transaction) cancel() {
	for ref := range t.logs {
		entry, exists := ref.node.kvs[ref.key]
		if exists && entry != nil {
			entry.lock.Unlock()
		}
	}
	t.cleanLocks()
}

// Delete removes KV entry from node.
func (t *Transaction) Delete(n *Node, key string) (err error) {
	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.
	ref := txnKeyRef{node: n, key: key}

	t.lock.RLock()
	trace := t.getCacheKV(ref, lc)
	t.lock.RUnlock()
	if trace != nil {
		trace.lc, trace.txn = lc, nil // mark deleted.
		return nil
	}

	trace = &txnLog{lc: lc}
	t.logs[ref] = trace

	return nil
}

// KV start kv transaction on node.
func (t *Transaction) KV(n *Node, key string) (txn KVTransaction, err error) {
	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.
	ref := txnKeyRef{node: n, key: key}

	t.lock.RLock()
	trace := t.getCacheKV(ref, lc)
	t.lock.RUnlock()
	if trace != nil && trace.txn != nil {
		return trace.txn, nil
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	if trace = t.getCacheKV(ref, lc); trace != nil {
		return trace.txn, nil
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

	validator, _ := t.Cluster.validators[key]

	if kver, _ := t.Cluster.engine.(TxnKVCoordinator); kver != nil {
		latestSnap, err := kver.TransactionBeginKV(t, n, key)
		if err != nil {
			return nil, err
		}
		// perfer coordinator provided snapshot.
		if latestSnap != nil {
			snap = latestSnap
		}
	}

	trace = &txnLog{
		lc:        lc,
		validator: validator,
		new:       false,
	}

	if snap == nil {
		// try local snapshot
		if e, exists := n.kvs[key]; !exists || e == nil {
			// new empty entry if not exist.
			snap, trace.new = &KeyValue{Key: key}, true

		} else {
			// lock this entry.
			e.lock.Lock()
			validator, existing, snap = e.validator, e, &e.KeyValue
		}
	}

	if validator == nil {
		return nil, ErrValidatorMissing
	}

	// create entry transaction
	if txn, err = validator.Txn(*snap); err != nil {
		if existing != nil {
			existing.lock.Unlock()
		}
		t.Cluster.log.Fatalf("validator of key \"%v\" failed to create transaction: " + err.Error())
	}
	// save
	trace.txn, t.logs[ref] = txn, trace

	// get real txn.
	if wrapper, wrapped := txn.(KVTransactionWrapper); wrapped {
		txn = wrapper.KVTransaction()
	}

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
