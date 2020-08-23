package sladder

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/sunmxt/sladder/proto"
)

var (
	ErrInvalidTransactionHandler = errors.New("invalid transaction handler")
	ErrTooManyNodes              = errors.New("too many nodes selected")
	ErrInvalidNode               = errors.New("invalid node")

	// ErrTransactionCommitViolation raises when any operation breaks commit limitation.
	ErrTransactionCommitViolation = errors.New("access violation in transaction commit")
	ErrTransactionStateBroken     = errors.New("a transaction state cannot rollback. cluster states broken")
)

// TransactionOperation contains an operation trace within transaction.
type TransactionOperation struct {
	LC uint32

	// Node fields
	Node                       *Node
	NodePastExists, NodeExists bool

	// Entry fields
	Key                         string
	Txn                         KVTransaction
	PastExists, Exists, Updated bool
}

func getExistance(new, txnUpdated, deleted bool) (pastExists, exists, updated bool) {
	return !new, !(new || deleted) || txnUpdated, txnUpdated
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
	TransactionCommit(*Transaction, []*TransactionOperation) (bool, error)
}

// TxnRollbackCoordinator traces transaction rollback.
type TxnRollbackCoordinator interface {
	TransactionRollback(*Transaction) error
}

// TxnCoordinator traces transaction.
type TxnCoordinator interface {
	TxnStartCoordinator
	TxnRollbackCoordinator
	TxnCommitCoordinator
	TxnKVCoordinator
}

type txnKeyRef struct {
	node *Node
	key  string
}

type txnLog struct {
	deletion bool
	txn      KVTransaction

	validator KVValidator
	lc        uint32
	new       bool
}

type nodeOpLog struct {
	deleted bool
	lc      uint32
}

const (
	txnFlagClusterLock = uint8(0x1)
)

type transactionFinalOp struct {
	lc uint32
	op func()
}

// Transaction contains read/write operations on cluster.
type Transaction struct {
	id      uint32
	lc      uint32
	Cluster *Cluster
	flags   uint8

	errs Errors

	lock         sync.RWMutex
	logs         map[txnKeyRef]*txnLog
	relatedNodes map[*Node]struct{}
	nodeOps      map[*Node]*nodeOpLog

	deferOps struct {
		normal     []*transactionFinalOp
		onCommit   []*transactionFinalOp
		onRollback []*transactionFinalOp
	}
}

func newTransaction(c *Cluster) *Transaction {
	return &Transaction{
		Cluster: c,
		lc:      0,
		logs:    make(map[txnKeyRef]*txnLog),
		nodeOps: make(map[*Node]*nodeOpLog),
	}
}

// ID returns transaction ID.
func (t *Transaction) ID() uint32 { return t.id }

// Prefail returns unrecoverable errors inside current transaction, mostly indicating a broken transaction.
func (t *Transaction) Prefail() error { return t.errs.AsError() }

// RangeRelatedNodes iterates nodes related to the transaction.
func (t *Transaction) RangeRelatedNodes(visit func(*Node) bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for node := range t.relatedNodes {
		if !visit(node) {
			return
		}
	}
}

// RelatedNodes return list of nodes related to the transaction.
func (t *Transaction) RelatedNodes() (nodes []*Node) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for node := range t.relatedNodes {
		nodes = append(nodes, node)
	}

	return
}

// TxnOption contains extra requirements for a transaction.
type TxnOption interface{}

type membershipModificationOption struct{}

// MembershipModification creates an option to enable membership changing.
func MembershipModification() TxnOption { return membershipModificationOption{} }

// Txn executes new transaction.
func (c *Cluster) Txn(do func(*Transaction) bool, opts ...TxnOption) (err error) {
	t, commit := newTransaction(c), true
	t.id = atomic.AddUint32(&c.transactionID, 1)

	for _, opt := range opts {
		switch opt.(type) {
		case membershipModificationOption:
			t.flags |= txnFlagClusterLock
		}
	}

	// TODO(xutao): deadlock detector.
	if t.flags&txnFlagClusterLock > 0 {
		c.lock.Lock()
		defer c.lock.Unlock()
	} else {
		c.lock.RLock()
		defer c.lock.RUnlock()
	}

	doFinalOps := func(finalOps []*transactionFinalOp) {
		sort.Slice(finalOps, func(i, j int) bool { return finalOps[i].lc <= finalOps[j].lc })
		for idx := 0; idx < len(finalOps); idx++ {
			finalOps[idx].op()
		}
	}

	// before transaction.
	if starter, _ := c.engine.(TxnStartCoordinator); starter != nil {
		if commit, err = starter.TransactionStart(t); err != nil {
			return err
		}
		if !commit {
			return ErrRejectedByCoordinator
		}
	}
	rollback := func() error {
		if rollbacker, _ := c.engine.(TxnRollbackCoordinator); rollbacker != nil {
			if err := rollbacker.TransactionRollback(t); err != nil {
				t.errs = append(t.errs, err)
			}
		}
		doFinalOps(t.cancel())
		return t.errs.AsError()
	}

	// do transaction.
	commit = do(t)
	if t.Prefail() != nil {
		commit = false
	}
	if !commit {
		return rollback()
	}

	err = t.deferNameChanges()
	if err != nil {
		t.errs = append(t.errs, err)
		return rollback()
	}

	// start commit
	if commiter, _ := c.engine.(TxnCommitCoordinator); commiter != nil {
		txns := make([]*TransactionOperation, 0, len(t.logs))
		for ref, log := range t.logs {
			rc := &TransactionOperation{
				Node: ref.node,
				Key:  ref.key,
				Txn:  log.txn,
				LC:   log.lc,
			}
			updated := log.txn.Updated()
			nodeOp, _ := t.nodeOps[ref.node]
			rc.PastExists, rc.Exists, rc.Updated = getExistance(log.new, updated, log.deletion)
			rc.NodePastExists, rc.NodeExists = nodeOp == nil || nodeOp.deleted, nodeOp == nil || !nodeOp.deleted
			txns = append(txns, rc)
		}
		for node, log := range t.nodeOps {
			rc := &TransactionOperation{
				Node:           node,
				LC:             log.lc,
				NodeExists:     !log.deleted,
				NodePastExists: log.deleted,
			}
			txns = append(txns, rc)
		}
		// sort by local lc.
		sort.Slice(txns, func(i, j int) bool {
			return txns[i].LC < txns[j].LC
		})
		if commit, err = commiter.TransactionCommit(t, txns); err != nil {
			t.errs = append(t.errs, err)
			return rollback()
		}
	}

	// save to local.
	if !commit {
		t.errs = append(t.errs, ErrRejectedByValidator)
		return rollback()
	}

	doFinalOps(t.commit())

	return nil
}

// DO NOT USE THIS DIRECTLY. Use NewNode() instead.
// This is only used for cluster initialization.
func (t *Transaction) _joinNode(node *Node) error {
	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.

	op := &nodeOpLog{deleted: false, lc: lc}
	t.nodeOps[node] = op

	return nil
}

// NewNode creates new node.
func (t *Transaction) NewNode() (node *Node, err error) {
	if err := t.Prefail(); err != nil { // reject in case of broken txn
		return nil, err
	}

	if !t.MembershipModification() {
		return nil, ErrTransactionCommitViolation
	}

	node = newNode(t.Cluster)
	return node, t._joinNode(node)
}

// MostPossibleNodeFromProtobuf finds most possible node to which the snapshot belongs.
func (t *Transaction) MostPossibleNodeFromProtobuf(entries []*proto.Node_KeyValue) (*Node, []string, error) {
	names, err := t.Cluster.resolveNodeNameFromProtobuf(entries)
	if err != nil {
		return nil, nil, err
	}
	if len(names) < 1 {
		return nil, nil, nil
	}
	return t.Cluster.mostPossibleNode(names), names, nil
}

// ResolveNodeNameFromProtobuf resolves node names from protobuf snapshot
func (t *Transaction) ResolveNodeNameFromProtobuf(entries []*proto.Node_KeyValue) ([]string, error) {
	return t.Cluster.resolveNodeNameFromProtobuf(entries)
}

// MostPossibleNode return the node whose names cover most of names in given set.
func (t *Transaction) MostPossibleNode(names []string) *Node {
	return t.Cluster.mostPossibleNode(names)
}

// RemoveNode removes node from cluster.
func (t *Transaction) RemoveNode(node *Node) (removed bool, err error) {
	if node == nil || node.cluster != t.Cluster {
		return false, ErrInvalidNode
	}
	if err := t.Prefail(); err != nil { // reject in case of broken txn
		return false, err
	}
	if !t.MembershipModification() {
		return false, ErrTransactionCommitViolation
	}

	op, _ := t.nodeOps[node]
	if op != nil {
		if !op.deleted {
			delete(t.nodeOps, node)
			removed = true
		}
	} else if t.Cluster.containNodes(node) {
		lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.
		op := &nodeOpLog{deleted: true, lc: lc}
		t.nodeOps[node] = op
		removed = true
	} else {
		return false, ErrInvalidNode
	}

	return removed, nil
}

// MembershipModification checks whether the transaction has permission to update node set.
func (t *Transaction) MembershipModification() bool { return t.flags&txnFlagClusterLock != 0 }

// Names returns names of node.
func (t *Transaction) Names(node *Node) []string {
	if node.cluster != t.Cluster {
		return nil
	}
	t.lockRelatedNode(node)
	return node.getNames()
}

func appendDeferOps(ps *[]*transactionFinalOp, fn func(), lc *uint32) {
	if fn == nil {
		return
	}
	*ps = append(*ps, &transactionFinalOp{
		lc: atomic.AddUint32(lc, 1),
		op: fn,
	})
}

// Defer registers a function called before transaction exits.
func (t *Transaction) Defer(fn func()) { appendDeferOps(&t.deferOps.normal, fn, &t.lc) }

// DeferOnCommit registers a function called before transaction commits.
func (t *Transaction) DeferOnCommit(fn func()) { appendDeferOps(&t.deferOps.onCommit, fn, &t.lc) }

// DeferOnRollback registers a function called before transaction rollbacks.
func (t *Transaction) DeferOnRollback(fn func()) { appendDeferOps(&t.deferOps.onRollback, fn, &t.lc) }

func (t *Transaction) deferNameChanges() (err error) {
	nameKeys := t.Cluster.resolver.Keys()

	updates := make(map[*Node]struct{})

	if len(nameKeys) > 0 {
		sort.Strings(nameKeys)
		for ref := range t.logs {
			if sort.SearchStrings(nameKeys, ref.key) >= 0 {
				updates[ref.node] = struct{}{}
			}
		}
	} else {
		// If resolver requires no entry, names of node should be resolved when a node newly joins cluster.
		for node, op := range t.nodeOps {
			if !op.deleted { // created
				updates[node] = struct{}{}
			}
		}
	}

	if len(updates) > 0 {
		t.Cluster.nodeIndexLock.Lock()
		defer t.Defer(t.Cluster.nodeIndexLock.Unlock)

		for node := range updates {
			var kvs []*KeyValue = nil
			if len(nameKeys) > 0 {
				kvs = make([]*KeyValue, 0, len(nameKeys))
				for _, key := range nameKeys {
					if !t.KeyExists(node, key) {
						continue
					}
					txn, err := t.KV(node, key)
					if err != nil {
						return err
					}
					value := txn.After()
					kvs = append(kvs, &KeyValue{Key: key, Value: value})
				}
			}
			names, err := t.Cluster.resolver.Resolve(kvs...)
			if err != nil {
				return err
			}
			if err = t.Cluster.updateNodeName(t, node, names); err != nil {
				return err
			}
		}

	}

	return nil
}

// RangeNode iterates over nodes.
func (t *Transaction) RangeNode(visit func(*Node) bool, excludeSelf, excludeEmpty bool) {
	if !excludeEmpty {
		for node, log := range t.nodeOps {
			if log == nil || log.deleted {
				continue
			}
			if !visit(node) {
				return
			}
		}
	}

	t.Cluster.rangeNodes(func(n *Node) bool {
		if log, _ := t.nodeOps[n]; log != nil && log.deleted {
			return true
		}
		return visit(n)
	}, excludeSelf, excludeEmpty)
}

func (t *Transaction) getCacheLog(ref txnKeyRef, lc uint32) *txnLog {
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

func (t *Transaction) lockRelatedNode(nodes ...*Node) {
	if t.relatedNodes == nil {
		t.relatedNodes = make(map[*Node]struct{})
	}

	for _, n := range nodes {
		if _, locked := t.relatedNodes[n]; !locked {
			n.lock.Lock()
			t.relatedNodes[n] = struct{}{}
		}
	}
}

func (t *Transaction) isRelatedNode(node *Node) bool {
	_, locked := t.relatedNodes[node]
	return locked
}

func (t *Transaction) cleanLocks() {
	// unlock all related nodes.
	for node := range t.relatedNodes {
		node.lock.Unlock()
	}
}

func (t *Transaction) commit() (finalOps []*transactionFinalOp) {
	removedEntries, removedRefs := make([]*KeyValueEntry, 0), make([]*txnKeyRef, 0)

	// node ops.
	for node, log := range t.nodeOps {
		if log == nil {
			continue
		}
		if log.deleted {
			t.Cluster._removeNode(node)
		} else {
			t.Cluster.emptyNodes[node] = struct{}{}
			t.Cluster.emitEvent(EmptyNodeJoined, node)
		}
	}

	for ref, log := range t.logs {
		entry, exists := ref.node.kvs[ref.key]
		if log.txn == nil {
			continue
		}

		updated, newValue := log.txn.Updated(), log.txn.After()
		deleted := log.deletion && !updated
		if exists && entry != nil { // exists.
			if deleted {
				delete(ref.node.kvs, ref.key) // remove
				removedEntries, removedRefs = append(removedEntries, entry), append(removedRefs, &ref)
			} else if updated { // updated.
				origin := entry.Value
				entry.Value = newValue
				t.Cluster.emitKeyChange(ref.node, entry.Key, origin, newValue, ref.node.keyValueEntries(true))
			}
			t.Defer(entry.lock.Unlock)

		} else if updated { // new: entry not exists and an update exists.
			entry = &KeyValueEntry{
				KeyValue: KeyValue{
					Key:   ref.key,
					Value: newValue,
				},
				validator: log.validator,
			}
			ref.node.kvs[ref.key] = entry
			t.Cluster.emitKeyInsertion(ref.node, entry.Key, newValue, ref.node.keyValueEntries(true))
		}
	}

	for idx, entry := range removedEntries { // send delete events.
		t.Cluster.emitKeyDeletion(removedRefs[idx].node, entry.Key, entry.Value, removedRefs[idx].node.keyValueEntries(true))
	}

	t.Defer(t.cleanLocks) // unlock all.

	finalOps = append(finalOps, t.deferOps.onCommit...)
	finalOps = append(finalOps, t.deferOps.normal...)

	return
}

func (t *Transaction) cancel() (finalOps []*transactionFinalOp) {
	for ref := range t.logs {
		entry, exists := ref.node.kvs[ref.key]
		if exists && entry != nil {
			t.Defer(entry.lock.Unlock)
		}
	}
	t.Defer(t.cleanLocks) // unlock all.
	finalOps = append(finalOps, t.deferOps.onRollback...)
	finalOps = append(finalOps, t.deferOps.normal...)
	return
}

// Delete removes KV entry from node.
func (t *Transaction) Delete(n *Node, key string) (err error) {
	if err := t.Prefail(); err != nil { // reject in case of broken txn
		return err
	}

	var log *txnLog

	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.

	t.lock.Lock()
	log, _, err = t.getLatestLog(n, key, true, lc)
	defer t.lock.Unlock()

	if err != nil {
		return err
	}

	if err = log.txn.SetRawValue(log.txn.Before()); err != nil {
		return err
	}
	log.deletion = true // mark deleted.

	return nil
}

func (t *Transaction) getLatestLog(n *Node, key string, create bool, lc uint32) (log *txnLog, created bool, err error) {
	ref := txnKeyRef{node: n, key: key}

	if log = t.getCacheLog(ref, lc); log != nil {
		return log, false, nil
	}

	if n.cluster != t.Cluster {
		return nil, false, ErrInvalidNode
	}

	if !create {
		return nil, false, nil
	}

	t.lockRelatedNode(n)

	var (
		snap *KeyValue
		txn  KVTransaction
	)

	validator, _ := t.Cluster.validators[key]

	if kver, _ := t.Cluster.engine.(TxnKVCoordinator); kver != nil {
		latestSnap, err := kver.TransactionBeginKV(t, n, key)
		if err != nil {
			return nil, false, err
		}
		// perfer coordinator provided snapshot.
		if latestSnap != nil {
			snap = latestSnap
		}
	}

	log = &txnLog{
		lc:        lc,
		validator: validator,
		new:       false,
	}

	if snap == nil {
		// try local snapshot
		if e, exists := n.kvs[key]; !exists || e == nil {
			// new empty entry if not exist.
			snap, log.new = &KeyValue{Key: key}, true

		} else {
			// lock this entry.
			e.lock.Lock()
			validator, snap = e.validator, &e.KeyValue

			defer func() {
				if err != nil {
					e.lock.Unlock()
				}
			}()
		}
	}

	if validator == nil {
		return nil, false, ErrValidatorMissing
	}

	// create entry transaction
	if txn, err = validator.Txn(*snap); err != nil {
		t.Cluster.log.Fatalf("validator of key \"%v\" failed to create transaction: " + err.Error())
		return nil, false, err
	}

	// save
	t.logs[txnKeyRef{key: key, node: n}], log.txn = log, txn

	return log, true, nil
}

func (t *Transaction) getKV(n *Node, key string, lc uint32) (KVTransaction, error) {
	log, _, err := t.getLatestLog(n, key, true, lc)
	if err != nil {
		return nil, err
	}
	txn := log.txn

	// get real txn.
	for {
		if wrapper, wrapped := txn.(KVTransactionWrapper); wrapped {
			txn = wrapper.KVTransaction()
			continue
		}
		break
	}

	return txn, nil
}

// KV start kv transaction on node.
func (t *Transaction) KV(n *Node, key string) (txn KVTransaction, err error) {
	if err := t.Prefail(); err != nil { // reject in case of broken txn
		return nil, err
	}

	lc := atomic.AddUint32(&t.lc, 1) // increase logic clock.
	ref := txnKeyRef{node: n, key: key}

	t.lock.RLock()
	log := t.getCacheLog(ref, lc)
	t.lock.RUnlock()
	if log != nil && log.txn != nil {
		return log.txn, nil
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	return t.getKV(n, key, lc)
}

// KeyExists checks whether keys exists in node.
func (t *Transaction) KeyExists(node *Node, keys ...string) bool {
	if node == nil || len(keys) < 1 {
		return true
	}
	if node.cluster != t.Cluster {
		return false
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	t.lockRelatedNode(node)

	for _, key := range keys {
		log, _ := t.logs[txnKeyRef{key: key, node: node}]
		if log != nil {
			if !(log.new || log.deletion) {
				continue
			}
			if log.txn.Updated() {
				continue
			}
			return false
		} else if _, exists := node.kvs[key]; !exists {
			return false
		}
	}

	return true
}

// RangeNodeKeys iterates over keys of node entries.
func (t *Transaction) RangeNodeKeys(n *Node, visit func(key string, pastExists bool) bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.rangeNodeKeys(n, visit)
}

// ReadNodeSnapshot creates node snapshot.
func (t *Transaction) ReadNodeSnapshot(node *Node, message *proto.Node) {
	if node == nil || message == nil {
		return
	}

	if node.cluster != t.Cluster {
		return
	}

	t.lock.RLock()
	if !t.isRelatedNode(node) {
		node.lock.RLock()
		defer node.lock.RUnlock()
	}
	t.lock.RUnlock()

	node.protobufSnapshot(message)
}

func (t *Transaction) rangeNodeKeys(n *Node, visitFn func(key string, pastExists bool) bool) {
	existingKeys := make(map[string]struct{}, len(n.kvs))

	visit := func(key string, pastExists bool) bool {
		existingKeys[key] = struct{}{}
		return visitFn(key, pastExists)
	}

	for _, entry := range n.kvs {
		log, exists := t.logs[txnKeyRef{key: entry.Key, node: n}]
		if exists {
			updated := log.txn.Updated()
			if _, exists, _ = getExistance(log.new, updated, log.deletion); !exists {
				continue
			}
		}
		if !visit(entry.Key, true) {
			return
		}
	}

	for ref, log := range t.logs {
		if ref.node != n {
			continue
		}
		if _, visited := existingKeys[ref.key]; visited {
			continue
		}
		updated := log.txn.Updated()
		pastExists, exists, _ := getExistance(log.new, updated, log.deletion)
		if !exists {
			continue
		}
		if !visit(ref.key, pastExists) {
			return
		}
	}
}
