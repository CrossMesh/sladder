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
		resolver:   resolver,
		engine:     engine,
		validators: make(map[string]KVValidator),
		nodes:      make(map[string]*Node),
		emptyNodes: make(map[*Node]struct{}),
		arbiter:    arbit.New(),
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
			for _, name := range n.names {
				if _, exists := c.nodes[name]; exists {
					continue checkNodes
				}
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
				c.log.Fatalf("transaction fails when removing a empty node. retry in 5 seconds. (err = \"%v\")", errs.AsError())
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

func (c *Cluster) updateNodeName(t *Transaction, n *Node, newNames []string) error {
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
	} else if len(newNames) < 1 { // all names removed.
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

	sort.Strings(newNames)
	var mergedNames []string

	deferIndexName := func(name string, node *Node) {
		t.DeferOnCommit(func() {
			c.nodes[name] = node
		})
	}

	// update name index.
	util.RangeOverStringSortedSet(n.names, newNames, func(s *string) bool {
		if s != nil { // remove old name index.
			t.DeferOnCommit(func() {
				delete(c.nodes, *s)
			})
		}
		return true
	}, func(s *string) bool {
		// new name.
		stored, exists := c.nodes[*s]
		if exists && stored != n { // ununique name.
			c.log.Warnf("drop ununique name %v of node with new names %v", *s, newNames)
			return true
		}
		deferIndexName(*s, n)
		mergedNames = append(mergedNames, *s)
		return true
	}, func(s *string) bool {
		mergedNames = append(mergedNames, *s)
		return true
	})

	// save to node.
	t.DeferOnCommit(func() {
		n.assignNames(mergedNames, true)
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
	// remove from empty node set.
	if _, exists := c.emptyNodes[node]; exists {
		delete(c.emptyNodes, node)
		c.emitEvent(NodeRemoved, node)
		return true
	}

	// remove from node set.
	lastNode := node
	for _, name := range node.names {
		if expected, exists := c.nodes[name]; !exists {
			continue
		} else if expected != node {
			if removed && lastNode != expected {
				// should not happen but log just in case.
				c.log.Warn("inconsisient node record with node ID \"%v\"", name)
			}
			continue
		} else {
			delete(c.nodes, name)
			lastNode = expected
			removed = true
		}
	}

	if removed {
		c.emitEvent(NodeRemoved, node)
	}

	return
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
