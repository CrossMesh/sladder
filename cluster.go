package sladder

import (
	"errors"
	"sort"
	"sync"

	arbit "github.com/sunmxt/arbiter"
	"github.com/sunmxt/sladder/proto"
	"github.com/sunmxt/sladder/util"
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

	nodes      map[string]*Node
	emptyNodes map[*Node]struct{}
	self       *Node

	arbiter *arbit.Arbiter

	// Whether node should be preserved when all node names removed.
	PreserveUnnamed bool

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

	// watch key for resolver.
	idKeys := resolver.Keys()
	if len(idKeys) > 0 {
		ctx := nc.Keys(idKeys...).Watch(nc.updateNodeID)
		if ctx == nil {
			msg := "got nil watch context when register watcher for resolver."
			nc.log.Fatal(msg)
			return nil, nil, errors.New(msg)
		}
	}

	if err = nc.joinNode(nc.self); err != nil {
		if ierr := engine.Close(); ierr != nil {
			panic(err)
		}
		return nil, nil, err
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
	return c.getNode(name)
}

// ContainNodes checks whether specfic nodes belong to cluster.
func (c *Cluster) ContainNodes(nodes ...interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

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

func (c *Cluster) mostPossibleNode(names []string) (most *Node) {
	if len(names) < 1 {
		return nil
	}

	hits, max := make(map[*Node]uint), uint(0)
	for _, name := range names {
		node := c.getNode(name)
		hit, exist := hits[node]
		if !exist {
			hit = 0
		}
		hit++
		hits[node] = hit
		if hit > max {
			most, max = node, hit
		}
	}
	return
}

func (c *Cluster) clearKey(key string) {
	nodeSet := make(map[*Node]struct{})

	for _, node := range c.nodes {
		if _, exists := nodeSet[node]; exists {
			continue
		}
		nodeSet[node] = struct{}{}

		node.lock.Lock()
		entry, exist := node.kvs[key]
		if exist {
			delete(node.kvs, key)
			c.emitKeyDeletion(node, entry.Key, entry.Value, node.keyValueEntries(true))
		}
		node.lock.Unlock()
	}
}

func (c *Cluster) replaceValidatorForce(key string, validator KVValidator) error {
	// safety: we hold exclusive lock of cluster scope here. node cannot create new entry now.
	// this keep validator of entry consistent.

	nodeSet := make(map[*Node]struct{})
	for _, node := range c.nodes {
		if _, exists := nodeSet[node]; exists {
			continue
		}
		nodeSet[node] = struct{}{}
		node.replaceValidatorForce(key, validator)
	}

	return nil
}

func (c *Cluster) replaceValidator(key string, validator KVValidator, forceReplace bool) error {
	if forceReplace {
		return c.replaceValidatorForce(key, validator)
	}

	nodeSet := make(map[*Node]struct{})
	c.rangeNodes(func(node *Node) bool {
		nodeSet[node] = struct{}{}
		return true
	}, false, false)

	for node := range nodeSet {
		// all nodes should be locked to prevent from entry changing.
		node.lock.Lock()
		defer node.lock.Unlock()

		entry := node.getEntry(key)
		if entry == nil {
			continue
		}
		// ensure that existing value is valid for new validator.
		if !validator.Validate(entry.KeyValue) {
			return ErrIncompatibleValidator
		}
	}

	// replace for all entry.
	for node := range nodeSet {
		entry := node.getEntry(key)
		if entry == nil {
			continue
		}
		entry.validator = validator
	}

	return nil
}

// RegisterKey registers key-value validator with specific key.
func (c *Cluster) RegisterKey(key string, validator KVValidator, forceReplace bool, flags uint32) error {
	// lock entire cluster.
	c.lock.Lock()
	defer c.lock.Unlock()

	_, exists := c.validators[key]
	if validator == nil {
		if !exists {
			return nil
		}
		// unregister. we need to drop existing key-values in nodes.
		c.clearKey(key)
		return nil
	}

	if exists {
		// replace validator.
		if err := c.replaceValidator(key, validator, forceReplace); err != nil {
			return err
		}
	}
	// assign the new
	c.validators[key] = validator

	return nil
}

func (c *Cluster) updateNodeID(ctx *WatchEventContext, meta KeyValueEventMetadata) {
	// TODO: deduplicate snapshots.
	c.updateNodeNameFromKV(meta.Node(), meta.Snapshot())
}

func (c *Cluster) updateNodeName(n *Node, newNames []string) {
	if n == nil {
		return
	}
	n.lock.Lock()
	defer n.lock.Unlock()

	if len(n.names) < 1 {
		if len(newNames) < 1 { // no name.
			c.emptyNodes[n] = struct{}{}
		} else { // has names.
			delete(c.emptyNodes, n)
			c.emitEvent(NodeJoined, n)
		}
	} else if len(newNames) < 1 { // all names removed.
		if !c.PreserveUnnamed && c.self != n {
			c.removeNode(n)
		} else {
			// move to empty node set.
			c.emptyNodes[n] = struct{}{}
		}
	}

	sort.Strings(newNames)
	var mergedNames []string

	// update name index.
	util.RangeOverStringSortedSet(n.names, newNames, func(s *string) bool {
		if s != nil { // remove old name index.
			delete(c.nodes, *s)
		}
		return true
	}, func(s *string) bool {
		// new name.
		stored, exists := c.nodes[*s]
		if exists && stored != n { // ununique name.
			c.log.Warnf("drop ununique name %v of node with new names %v", *s, newNames)
			return true
		}
		c.nodes[*s] = n
		mergedNames = append(mergedNames, *s)
		return true
	}, func(s *string) bool {
		mergedNames = append(mergedNames, *s)
		return true
	})

	// save to node.
	n.assignNames(mergedNames, true)
}

// NewNode creates an new empty node in cluster.
// The new will not be append to cluster until name resolved.
func (c *Cluster) NewNode() (*Node, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.newNode()
}

func (c *Cluster) joinNode(n *Node) error {
	c.emitEvent(EmptyNodeJoined, n)

	names, err := c.resolver.Resolve(n.KeyValueEntries(false)...)
	if err != nil {
		return err
	}
	c.updateNodeName(n, names)

	return nil
}

func (c *Cluster) updateNodeNameFromKV(n *Node, snap []*KeyValue) {
	c.lock.Lock()
	defer c.lock.Unlock()

	newNames, err := c.resolver.Resolve(snap...)
	if err != nil {
		c.log.Fatal("resolver failure when node names update, got " + err.Error())
		return
	}
	c.updateNodeName(n, newNames)
}

func (c *Cluster) newNode() (n *Node, err error) {
	n = newNode(c)
	if err = c.joinNode(n); err != nil {
		return nil, err
	}
	return n, nil
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

func (c *Cluster) removeNode(node *Node) (removed bool) {
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
func (c *Cluster) RemoveNode(node *Node) (removed bool) {
	if node == nil {
		return false
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	node.lock.Lock()
	defer node.lock.Unlock()

	return c.removeNode(node)
}

// Quit sends "leave" message to cluster and shops member sync.
func (c *Cluster) Quit() error {
	return c.engine.Close()
}
