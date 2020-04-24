package sladder

import (
	"errors"
	"sync"

	"github.com/sunmxt/sladder/proto"
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

	log Logger
}

const (
	// LocalEntry will not be synced to remote.
	LocalEntry = uint32(0x1)
)

// EngineOption contains engine-specific parameters.
type EngineOption interface{}

// NewClusterWithNameResolver creates new cluster.
func NewClusterWithNameResolver(engine EngineInstance, resolver NodeNameResolver, logger Logger) (c *Cluster, self *Node, err error) {
	if resolver == nil {
		return nil, nil, ErrMissingNameResolver
	}
	if logger == nil {
		logger = DefaultLogger
	}
	c = &Cluster{
		resolver:      resolver,
		engine:        engine,
		validators:    make(map[string]KVValidator),
		nodes:         make(map[string]*Node),
		emptyNodes:    make(map[*Node]struct{}),
		eventRegistry: newEventRegistry(),
		log:           logger,
	}
	c.self = newNode(c)

	// init engine for cluster.
	if err = engine.Init(c); err != nil {
		return nil, nil, err
	}

	if err = c.joinNode(c.self); err != nil {
		if ierr := engine.Close(); ierr != nil {
			panic(err)
		}
		return nil, nil, err
	}

	return c, c.self, nil
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
		hits[node] = hit + 1
		if hit >= max {
			most = node
			max = hit + 1
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

		delete(node.kvs, key)
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
	// all nodes should be locked to prevent from entry changing.
	for _, node := range c.nodes {
		if _, exists := nodeSet[node]; exists {
			continue
		}
		nodeSet[node] = struct{}{}
		node.lock.Lock()
		defer node.lock.Unlock()
	}

	// ensure that existing value is valid for new validator.
	for node := range nodeSet {
		entry := node.get(key)
		if entry == nil {
			continue
		}
		if !validator.Validate(entry.KeyValue) {
			return ErrIncompatibleValidator
		}
	}

	// replace for all entry.
	for node := range nodeSet {
		entry := node.get(key)
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

func (c *Cluster) registerNode(names []string, n *Node) {
	if n != nil {
		for _, name := range names {
			c.nodes[name] = n
		}
		n.names = names
		c.emitEvent(NodeJoined, n)
		return
	}

	for _, name := range names {
		delete(c.nodes, name)
	}
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
	if len(names) > 0 {
		// has valid names. register to cluster.
		c.registerNode(names, n)

	} else {
		c.emptyNodes[n] = struct{}{}
	}

	return nil
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
	return (&OperationContext{}).Nodes(nodes...)
}

// RangeNodes iterate nodes.
func (c *Cluster) RangeNodes(visit func(*Node) bool, excludeSelf bool) {
	if visit == nil {
		return
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	c.rangeNodes(visit, excludeSelf)
}

func (c *Cluster) rangeNodes(visit func(*Node) bool, excludeSelf bool) {
	nameSet := make(map[string]struct{}, len(c.nodes))
	for name, node := range c.nodes {
		if _, exist := nameSet[name]; exist {
			continue
		}
		nameSet[name] = struct{}{}
		if excludeSelf && node == c.self {
			continue
		}
		if !visit(node) {
			break
		}
	}
}

// ProtobufSnapshot creates a snapshot of cluster in protobuf format.
func (c *Cluster) ProtobufSnapshot(s *proto.Cluster) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.protobufSnapshot(s)
}

func (c *Cluster) protobufSnapshot(s *proto.Cluster) {
	if s == nil {
		s = &proto.Cluster{}
	}

	c.rangeNodes(func(n *Node) bool {
		ns := &proto.Node{}
		n.ProtobufSnapshot(ns)
		s.Nodes = append(s.Nodes, ns)
		return true
	}, false)
}

//func (c *Cluster) rangeNodesStream(excludeSelf bool) <-chan *Node {
//	ch := make(chan *Node)
//
//	go func() {
//		ctx, cancelCtx := context.WithDeadline(context.TODO(), time.Now().Add(time.Minute))
//
//		defer close(ch)
//		defer cancelCtx()
//
//		c.rangeNodes(func(node *Node) bool {
//			select {
//			case ch <- node:
//			case <-ctx.Done():
//				// take too much time to finish. may be freezing or deadlocked.
//				c.log.Warn("rangeNodesStream deadline exceeded. terminate channel for safety. \n" + string(debug.Stack()))
//				return false
//			}
//			return true
//		}, excludeSelf)
//	}()
//	return ch
//}

// RemoveNode removes node from cluster.
func (c *Cluster) RemoveNode(node *Node) (removed bool) {
	if node == nil {
		return false
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	// remove from empty node set.
	if _, exists := c.emptyNodes[node]; exists {
		delete(c.emptyNodes, node)
		return true
	}

	// remove from node set.
	var lastNode *Node
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

	return
}

// Quit sends "leave" message to cluster and shops member sync.
func (c *Cluster) Quit() error {
	return nil
}
