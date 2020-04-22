package sladder

import (
	"errors"
	"sync"
)

var (
	ErrMissingNameResolver   = errors.New("missing node name resolver")
	ErrIncompatibleValidator = errors.New("validator is not compatiable for existing data")
)

// NodeNameResolver extracts node identifiers.
type NodeNameResolver interface {
	Name(*Node) ([]string, error)
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

	resolver   NodeNameResolver
	engine     EngineInstance
	validators map[string]KVValidator

	eventHandlers             map[*ClusterEventContext]struct{}
	keyEventWatcherIndex      map[string]map[*OperationContext]struct{}
	nodeNameEventWatcherIndex map[string]map[*OperationContext]struct{}
	nodeEventWatcherIndex     map[*Node]struct{}

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
		logger = defaultLogger
	}
	c = &Cluster{
		resolver:      resolver,
		engine:        engine,
		validators:    make(map[string]KVValidator),
		nodes:         make(map[string]*Node),
		emptyNodes:    make(map[*Node]struct{}),
		eventHandlers: make(map[*ClusterEventContext]struct{}),
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

	names, err := c.resolver.Name(n)
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
	go func() {
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
	}()
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
