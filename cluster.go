package sladder

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/crossmesh/sladder/proto"
	"github.com/crossmesh/sladder/util"
	arbit "github.com/sunmxt/arbiter"
)

var (
	ErrMissingNameResolver   = errors.New("missing node name resolver")
	ErrIncompatibleValidator = errors.New("validator is not compatiable for existing data")
)

// NodeNameResolver extracts node identifiers.
type NodeNameResolver interface {
	Resolve(...*KeyValue) ([]string, error)
	Keys() []string
}

// EngineInstance is live instance of engine.
type EngineInstance interface {
	Init(*Cluster) error
	Close() error
}

// Engine implements underlay membership protocol driver.
type Engine interface {
	New(...EngineOption) (EngineInstance, error)
}

// Cluster contains a set of node.
type Cluster struct {
	lock sync.RWMutex

	resolver   NodeNameResolver
	engine     EngineInstance
	validators map[string]KVValidator

	*eventRegistry

	nodeIndexLock sync.RWMutex
	nodes         map[string]*Node
	conflictNodes map[*Node]struct{}
	emptyNodes    map[*Node]struct{}

	self *Node

	arbiter *arbit.Arbiter

	// Whether node should be preserved when all node names removed.
	PreserveUnnamed bool

	transactionID uint32

	log Logger
}

// EngineOption contains engine-specific parameters.
type EngineOption interface{}

// ClusterOption contains cluster parameters.
type ClusterOption interface{}

type preserveUnnamedOption bool

// PreserveUnnamedNode is option indicating whether unnamed node should be preserved.
func PreserveUnnamedNode(x bool) ClusterOption { return PreserveUnnamedNode(x) }

// NewClusterWithNameResolver creates new cluster.
func NewClusterWithNameResolver(engine EngineInstance, resolver NodeNameResolver, options ...ClusterOption) (c *Cluster, self *Node, err error) {
	var logger Logger

	if resolver == nil {
		return nil, nil, ErrMissingNameResolver
	}

	nc := &Cluster{
		resolver:      resolver,
		engine:        engine,
		validators:    make(map[string]KVValidator),
		nodes:         make(map[string]*Node),
		emptyNodes:    make(map[*Node]struct{}),
		conflictNodes: make(map[*Node]struct{}),
		arbiter:       arbit.New(),
	}

	for _, opt := range options {
		switch o := opt.(type) {
		case Logger:
			logger = o
		case preserveUnnamedOption:
			nc.PreserveUnnamed = bool(o)
		}
	}
	if logger == nil {
		logger = DefaultLogger
	}
	nc.log = logger

	nc.self = newNode(nc)
	nc.eventRegistry = newEventRegistry(nc.arbiter)

	// init engine for cluster.
	if err = engine.Init(nc); err != nil {
		return nil, nil, err
	}

	nc.startWorker()

	defer func() {
		// terminate all in case of any failure.
		if err != nil {
			nc.arbiter.Shutdown()
			nc.arbiter.Join()
		}
	}()

	var errs Errors

	// add initial self node.
	if err = nc.Txn(func(t *Transaction) bool {
		if ierr := t._joinNode(nc.self); ierr != nil {
			errs = append(errs, ierr)
			return false
		}
		return true
	}, MembershipModification()); err != nil {
		errs = append(errs, err)
	}

	if errs.AsError() != nil {
		if ierr := engine.Close(); ierr != nil {
			panic(ierr)
		}
		return nil, nil, errs.AsError()
	}

	return nc, nc.self, nil
}

// Self returns self node.
func (c *Cluster) Self() *Node {
	return c.self
}

func (c *Cluster) getNode(name string) *Node {
	n, _ := c.nodes[name]
	return n
}

// GetNode find node from name.
func (c *Cluster) GetNode(name string) *Node {
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.nodeIndexLock.RLock()
	defer c.nodeIndexLock.RUnlock()

	return c.getNode(name)
}

// ContainNodes checks whether specfic nodes belong to cluster.
func (c *Cluster) ContainNodes(nodes ...interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.nodeIndexLock.RLock()
	defer c.nodeIndexLock.RUnlock()

	return c.containNodes(nodes...)
}

func (c *Cluster) getRealEntry(kv *KeyValue) *KeyValue {
	if kv == nil {
		return nil
	}
	validator, _ := c.validators[kv.Key]
	if validator == nil {
		// no validator. treat it unwrapped.
		return kv
	}
	txn, err := validator.Txn(*kv)
	if err != nil {
		// KVValidator must ensure invalid value will not be stored, so normally this should not happen.
		// But we report the error and treat entry unwrapped in case.
		c.log.Errorf("failed to start KVTransaction. (err = %v)", err)
		return kv
	}
	real := getRealTransaction(txn)
	if real == txn {
		return kv
	}
	return &KeyValue{
		Key:   kv.Key,
		Value: real.Before(),
	}
}

func (c *Cluster) containNodes(nodes ...interface{}) bool {
checkNodes:
	for _, raw := range nodes {
		switch n := raw.(type) {
		case string:
			if c.getNode(n) == nil {
				return false
			}

		case *Node:
			if n == c.self {
				continue checkNodes
			}
			if _, exists := c.emptyNodes[n]; exists {
				continue checkNodes
			}
			if _, exists := c.conflictNodes[n]; exists {
				continue checkNodes
			}
			found := false
			for _, name := range n.names {
				if node, exists := c.nodes[name]; exists && node == n {
					found = true
					continue checkNodes
				} else if found {
					c.log.Error("[BUG!] conflictNodes doesn't contains a node with name conflict.")
					break
				}
			}
			if found {
				continue checkNodes
			}
			return false
		}
	}

	return true
}

// MostPossibleNode return the node whose names cover most of names in given set.
func (c *Cluster) MostPossibleNode(names []string) *Node {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.mostPossibleNode(names)
}

// MostPossibleNode return the node whose names cover most of names in given set.
func MostPossibleNode(names []string, getNode func(name string) *Node) (possible *Node) {
	if len(names) < 1 {
		return nil
	}
	hits, max := make(map[*Node]uint), uint(0)
	for _, name := range names {
		node := getNode(name)
		hit, exist := hits[node]
		if !exist {
			hit = 0
		}
		hit++
		hits[node] = hit
		if hit > max {
			possible, max = node, hit
		}
	}
	return
}

func searchNodeForMerge(
	n *Node,
	getNode func(name string) []*Node,
	getName func(n *Node) []string) (source []*Node, conflicts []*Node) {
	names := n.names
	if len(names) < 1 {
		return nil, nil
	}

	var hits map[*Node]uint

	hits = make(map[*Node]uint)
	for _, name := range names {
		for _, node := range getNode(name) {
			if node == n {
				continue
			}
			hit, exist := hits[node]
			if !exist {
				hit = 0
			}
			hit++
			hits[node] = hit
		}
	}

	if len(hits) < 1 {
		return nil, nil
	}

	sort.Strings(names)

	fullContained := true
	notContains := func(s *string) bool {
		fullContained = false
		return false
	}
	for node := range hits {
		fullContained = true
		util.RangeOverStringSortedSet(names, node.names, nil, notContains, nil)
		if fullContained {
			source = append(source, node)
		} else {
			conflicts = append(conflicts, node)
		}
	}
	return
}

func (c *Cluster) mostPossibleNode(names []string) *Node {
	return MostPossibleNode(names, c.getNode)
}

// RegisterKey registers key-value validator with specific key.
func (c *Cluster) RegisterKey(key string, validator KVValidator, forceReplace bool, flags uint32) error {
	var errs Errors

	if err := c.Txn(func(t *Transaction) bool {
		_, exists := c.validators[key]
		if validator == nil {
			if !exists {
				return false
			}
		}

		nodes := make([]*Node, 0)
		t.RangeNode(func(node *Node) bool {
			nodes = append(nodes, node)
			return true
		}, false, false)

		for _, node := range nodes {
			// replace validator.
			if err := node.replaceValidator(t, key, validator, forceReplace); err != nil {
				errs = append(errs, err)
				return false
			}
		}

		// assign the new
		t.DeferOnCommit(func() {
			c.validators[key] = validator
		})

		return true
	}, MembershipModification()); err != nil {
		errs = append(errs, err)
	}

	return errs.AsError()
}

func (c *Cluster) delayRemoveNode(n *Node) {
	c.eventRegistry.queueLock.Lock()
	defer c.eventRegistry.queueLock.Unlock()

	c.eventRegistry.enqueueWork(func() {
		var errs Errors

		if err := c.Txn(func(t *Transaction) bool {
			if len(t.Names(n)) > 0 {
				return false
			}
			if _, err := t.RemoveNode(n); err != nil && err != ErrInvalidNode {
				errs = append(errs, err)
				return false
			}
			return true
		}, MembershipModification()); err != nil {
			errs = append(errs, err)
		}

		if err := errs.AsError(); err != nil {
			// should not happen but just in case.
			c.arbiter.Go(func() {
				c.log.Errorf("transaction fails when removing a empty node. retry in 5 seconds. (err = \"%v\")", errs.AsError())
				errs = nil
				select {
				case <-time.After(time.Second):
					c.delayRemoveNode(n)
				case <-c.arbiter.Exit():
				}
			})
		}
	})
}

func (c *Cluster) solvePossibleNameConflict() {
	for node := range c.conflictNodes {
		solved := true
		for _, name := range node.names {
			if stored, exists := c.nodes[name]; exists {
				if stored != node {
					solved = false
				}
			} else {
				c.nodes[name] = node
			}
		}

		if solved {
			delete(c.conflictNodes, node)
		}
	}
}

func (c *Cluster) delayMergeProc(hins []*Node, delay time.Duration) {
	if delay > 0 {
		c.arbiter.Go(func() {
			time.Sleep(delay)
			c.delayMergeProc(hins, 0)
		})
		return
	}

	c.eventRegistry.queueLock.Lock()
	defer c.eventRegistry.queueLock.Unlock()

	c.eventRegistry.enqueueWork(func() {
		if err := c.Txn(func(t *Transaction) bool {
			return c.searchAndMergeNodes(t, hins, false)
		}, MembershipModification()); err != nil {
			c.log.Errorf("cannot merge nodes. failed to commit transaction. retry later. (err = \"%v\")", err)
			c.delayMergeProc(hins, time.Second*5)
		}
	})
}

func (c *Cluster) searchAndMergeNodes(t *Transaction, hins []*Node, async bool) (merged bool) {
	var nameMap map[string][]*Node

	buildHins := hins == nil
	if !buildHins && len(hins) < 1 {
		return false
	}

	if !t.MembershipModification() { // cannot merge in current transaction.
		async = true
	}

	if len(c.conflictNodes) > 0 {
		nameMap = make(map[string][]*Node, len(c.nodes))
	}

	for name, node := range c.nodes {
		if buildHins {
			hins = append(hins, node)
		}
		if nameMap != nil {
			nameMap[name] = []*Node{node}
		}
	}
	for node := range c.conflictNodes {
		partialConflict := false
		for _, name := range node.names {
			if stored, exists := c.nodes[name]; exists && stored != node {
				nodes, _ := nameMap[name]
				nodes = append(nodes, node)
				nameMap[name] = nodes
			} else {
				partialConflict = true
			}
		}
		if buildHins && !partialConflict {
			hins = append(hins, node)
		}
	}

	nameFromMap := func(name string) []*Node {
		if nameMap != nil {
			nodes, _ := nameMap[name]
			return nodes
		}
		node, _ := c.nodes[name]
		if node != nil {
			return []*Node{node}
		}
		return nil
	}

	// search for targets.
	mergeMap := make(map[*Node]*Node)
	for _, node := range hins {
		sources, _ := searchNodeForMerge(node, nameFromMap, func(n *Node) []string {
			return n.names
		})
		if len(sources) < 1 {
			continue
		}
		if async { // force async.
			c.delayMergeProc(hins, 0)
			return true
		}
		for _, source := range sources {
			mergeMap[source] = node
		}
	}

	if len(mergeMap) < 1 {
		return false // nothing to merge.
	}

	// cut the loop edge. (if any)
	dfs := make(map[*Node]*Node, len(mergeMap))
	for enter := range mergeMap {
		vertex := enter
		for {
			dfs[vertex] = enter
			next, _ := mergeMap[vertex]
			if next == nil {
				break
			}
			if mark, _ := dfs[next]; mark == enter {
				if next != c.self { // self shouldn't be merging source. preserve this edge.
					delete(mergeMap, vertex) // cut edge.
				}
				break
			}
			vertex = next
		}
	}
	dfs = nil // dereference.

	// combine merging chain.
	// for example, A <-- B <-- C will be reduced to A <-- B and A <-- C.
	for source, target := range mergeMap {
		if source == c.self {
			// hummmm... myself
			delete(mergeMap, source)
			continue
		}
		actualTarget := target
		for {
			parent, hasParent := mergeMap[actualTarget]
			if !hasParent || parent == nil {
				break
			}
			actualTarget = parent
		}
		if actualTarget != target {
			mergeMap[source] = actualTarget
		}
	}

	// merge.
	if hins != nil {
		hins = hins[:0]
	}
	for source, target := range mergeMap {
		if ok, err := t.mergeNode(target, source); err != nil {
			c.log.Errorf("failed to merge node. [target = %v, source = %v] (err = \"%v\")", t.Names(target), t.Names(source), err)
			hins = append(hins, source)
		} else if ok {
			merged = true
		}
	}

	if len(hins) > 0 {
		c.delayMergeProc(hins, 0)
	}

	return
}

func (c *Cluster) deferUpdateNodeName(t *Transaction, n *Node, newNames []string, isNodeDelete bool) error {
	if len(n.names) < 1 {
		if len(newNames) < 1 { // no name.
			t.DeferOnCommit(func() {
				c.emptyNodes[n] = struct{}{}
			})
		} else { // has names.
			t.DeferOnCommit(func() {
				delete(c.emptyNodes, n)
				c.emitEvent(NodeJoined, n)
			})
		}
	} else if len(newNames) < 1 && !isNodeDelete { // all names removed.
		if !c.PreserveUnnamed && c.self != n {
			// this transaction has no permission to remove node. delay it.
			if !t.MembershipModification() {
				c.delayRemoveNode(n)
			} else if _, err := t.RemoveNode(n); err != nil {
				return err
			}
		} else {
			t.DeferOnCommit(func() {
				// move to empty node set.
				c.emptyNodes[n] = struct{}{}
			})
		}
	}

	t.DeferOnCommit(func() {
		sort.Strings(newNames)
		var mergedNames []string

		// update name index.
		util.RangeOverStringSortedSet(n.names, newNames, func(s *string) bool {
			if s != nil { // remove old name index.
				node, _ := c.nodes[*s]
				if node == n {
					delete(c.nodes, *s)
				}
			}
			return true
		}, func(s *string) bool {
			// new name.
			mergedNames = append(mergedNames, *s)
			if stored, exists := c.nodes[*s]; exists && stored != n { // conflicts.
				t.Cluster.conflictNodes[n] = struct{}{}
			} else {
				c.nodes[*s] = n
			}
			return true
		}, func(s *string) bool {
			mergedNames = append(mergedNames, *s)
			return true
		})

		if !isNodeDelete {
			// save to node.
			n.assignNames(mergedNames, true)
		}
	})

	return nil
}

// NewNode creates an new empty node in cluster.
func (c *Cluster) NewNode() (node *Node, err error) {
	var errs Errors

	if err = c.Txn(func(t *Transaction) bool {
		node, err = t.NewNode()
		if err != nil {
			errs = append(errs, err)
			return false
		}
		return true
	}, MembershipModification()); err != nil {
		errs = append(errs, err)
	}

	if err = errs.AsError(); err != nil {
		return nil, err
	}
	return node, nil
}

// Keys creates operation context
func (c *Cluster) Keys(keys ...string) *OperationContext {
	return (&OperationContext{cluster: c}).Keys(keys...)
}

// Nodes creates operation context
func (c *Cluster) Nodes(nodes ...interface{}) *OperationContext {
	return (&OperationContext{cluster: c}).Nodes(nodes...)
}

// RangeNodes iterate nodes.
func (c *Cluster) RangeNodes(visit func(*Node) bool, excludeSelf, excludeEmpty bool) {
	if visit == nil {
		return
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.nodeIndexLock.RLock()
	defer c.nodeIndexLock.RUnlock()

	c.rangeNodes(visit, excludeSelf, excludeEmpty)
}

func (c *Cluster) rangeNodes(visit func(*Node) bool, excludeSelf, excludeEmpty bool) {
	nodeSet := make(map[*Node]struct{}, len(c.nodes))
	for _, node := range c.nodes {
		if _, exist := nodeSet[node]; exist {
			continue
		}
		nodeSet[node] = struct{}{}
		if excludeSelf && node == c.self {
			continue
		}
		if !visit(node) {
			break
		}
	}
	if !excludeEmpty {
		for node := range c.emptyNodes {
			if !visit(node) {
				break
			}
		}
	}
}

// ProtobufSnapshot creates a snapshot of cluster in protobuf format.
func (c *Cluster) ProtobufSnapshot(s *proto.Cluster, validate func(*Node) bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.protobufSnapshot(s, validate)
}

func (c *Cluster) protobufSnapshot(s *proto.Cluster, validate func(*Node) bool) {
	if s == nil {
		return
	}

	if len(s.Nodes) > 0 {
		s.Nodes = s.Nodes[0:0]
	}

	c.rangeNodes(func(n *Node) bool {
		if validate != nil && !validate(n) {
			return true
		}
		ns := &proto.Node{}
		n.ProtobufSnapshot(ns)
		s.Nodes = append(s.Nodes, ns)
		return true
	}, false, true)
}

func (c *Cluster) _removeNode(node *Node) (removed bool) {
	defer c.emitEvent(NodeRemoved, node)

	// remove from empty node set.
	if _, exists := c.emptyNodes[node]; exists {
		delete(c.emptyNodes, node)
		return true
	}

	delete(c.conflictNodes, node)

	return true
}

// RemoveNode removes node from cluster.
func (c *Cluster) RemoveNode(node *Node) (removed bool, err error) {
	if node == nil {
		return false, nil
	}

	var errs Errors

	if err = c.Txn(func(t *Transaction) bool {
		if removed, err = t.RemoveNode(node); err != nil {
			errs = append(errs, err)
			return false
		}
		return true
	}, MembershipModification()); err != nil {
		errs = append(errs, err)
	}

	err = errs.AsError()
	return removed && err == nil, err
}

// Quit leaves cluster.
func (c *Cluster) Quit() error {
	return c.engine.Close()
}
