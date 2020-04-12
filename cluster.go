package sladder

import (
	"errors"
	"sync"
)

var (
	ErrMissingNameResolver     = errors.New("missing node name resolver")
	ErrIncompatibleCoordinator = errors.New("coordinator is not compatiable for existing data")
)

// NodeNameResolver extracts node identifiers.
type NodeNameResolver interface {
	Name(*Node) ([]string, error)
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

// Event is enum type of event in cluster scope.
type Event uint

const (
	// EmptyNodeJoined tiggered after an empty node(with no name) joins cluster.
	EmptyNodeJoined = Event(1)
	// NodeJoined tiggered after node name is resolved and the node joins cluster.
	NodeJoined = Event(2)
)

// ClusterEventHandler receives events of cluster.
type ClusterEventHandler func(*ClusterEventContext, Event, *Node)

// ClusterEventContext refers to event handler.
type ClusterEventContext struct {
	handler      ClusterEventHandler
	unregistered bool
}

// Unregister cancels event handler.
func (c *ClusterEventContext) Unregister() { c.unregistered = true }

// Cluster contains a set of node.
type Cluster struct {
	lock sync.RWMutex

	resolver      NodeNameResolver
	engine        EngineInstance
	coordinators  map[string]KVCoordinator
	eventHandlers map[*ClusterEventContext]struct{}
	nodes         map[string]*Node
	emptyNodes    map[*Node]struct{}
	self          *Node

	log Logger
}

// EngineOption contains engine-specific parameters.
type EngineOption interface{}

// NewClusterWithNameResolver creates new cluster.
func NewClusterWithNameResolver(engine EngineInstance, resolver NodeNameResolver, logger Logger) (c *Cluster, self *Node, err error) {
	if resolver == nil {
		return nil, nil, ErrMissingNameResolver
	}
	if logger == nil {
		logger = defaultLogger
	}
	c = &Cluster{
		resolver:      resolver,
		engine:        engine,
		coordinators:  make(map[string]KVCoordinator),
		nodes:         make(map[string]*Node),
		emptyNodes:    make(map[*Node]struct{}),
		eventHandlers: make(map[*ClusterEventContext]struct{}),
		log:           logger,
	}

	// init engine for cluster.
	if err = engine.Init(c); err != nil {
		return nil, nil, err
	}
	if self, err = c.NewNode(); err != nil {
		return nil, nil, err
	}
	c.self = self

	return
}

// Self returns self node.
func (c *Cluster) Self() *Node {
	return c.self
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

func (c *Cluster) replaceCoordinatorForce(key string, coordinator KVCoordinator) error {
	// safety: we hold exclusive lock of cluster scope here. node cannot create new entry now.
	// this keep coordinator of entry consistent.

	nodeSet := make(map[*Node]struct{})
	for _, node := range c.nodes {
		if _, exists := nodeSet[node]; exists {
			continue
		}
		nodeSet[node] = struct{}{}
		node.replaceCoordinatorForce(key, coordinator)
	}

	return nil
}

func (c *Cluster) replaceCoordinator(key string, coordinator KVCoordinator, forceReplace bool) error {

	if forceReplace {
		return c.replaceCoordinatorForce(key, coordinator)
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

	// ensure that existing value is valid for new coordinator.
	for node := range nodeSet {
		entry := node.get(key)
		if entry == nil {
			continue
		}
		if !coordinator.Validate(entry.KeyValue) {
			return ErrIncompatibleCoordinator
		}
	}

	// replace for all entry.
	for node := range nodeSet {
		entry := node.get(key)
		if entry == nil {
			continue
		}
		entry.coordinator = coordinator
	}

	return nil
}

// RegisterKey registers key-value coordinator with specific key.
func (c *Cluster) RegisterKey(key string, coordinator KVCoordinator, forceReplace bool) error {
	// lock entire cluster.
	c.lock.Lock()
	defer c.lock.Unlock()

	_, exists := c.coordinators[key]
	if coordinator == nil {
		if !exists {
			return nil
		}
		// unregister. we need to drop existing key-values in nodes.
		c.clearKey(key)
	}

	if exists {
		// replace coordinator.
		if err := c.replaceCoordinator(key, coordinator, forceReplace); err != nil {
			return err
		}
	}
	// assign the new
	c.coordinators[key] = coordinator

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

	n := newNode(c)

	c.emitEvent(EmptyNodeJoined, n)

	names, err := c.resolver.Name(n)
	if err != nil {
		return nil, err
	}
	if len(names) > 0 {
		// has valid names. register to cluster.
		c.registerNode(names, n)
	} else {
		c.emptyNodes[n] = struct{}{}
	}

	return n, nil
}

// Keys creates key context
func (c *Cluster) Keys(keys ...string) *KeyContext {
	return nil
}

// Watch registers event handler of cluster.
func (c *Cluster) Watch(handler ClusterEventHandler) *ClusterEventContext {
	if handler == nil {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	ctx := &ClusterEventContext{
		handler:      handler,
		unregistered: false,
	}
	c.eventHandlers[ctx] = struct{}{}

	return ctx
}

func (c *Cluster) emitEvent(event Event, node *Node) {
	// call handlers.
	for ctx := range c.eventHandlers {
		if ctx.unregistered {
			delete(c.eventHandlers, ctx)
			continue
		}

		ctx.handler(ctx, event, node)

		if ctx.unregistered {
			delete(c.eventHandlers, ctx)
		}
	}
}

// RangeNodes iterate nodes.
func (c *Cluster) RangeNodes(visit func(*Node) bool, excludeSelf bool) {
	if visit == nil {
		return
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
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
