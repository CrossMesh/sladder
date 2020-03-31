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

// EngineOption contains engine-specific parameters.
type EngineOption interface{}

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

	resolver     NodeNameResolver
	engine       EngineInstance
	coordinators map[string]KVCoordinator
	nodes        map[string]*Node
	emptyNodes   map[*Node]struct{}

	log Logger
}

// NewClusterWithNameResolver creates new cluster.
func NewClusterWithNameResolver(engine EngineInstance, resolver NodeNameResolver, logger Logger) (c *Cluster, err error) {
	if resolver == nil {
		return nil, ErrMissingNameResolver
	}
	c = &Cluster{
		resolver:     resolver,
		engine:       engine,
		coordinators: make(map[string]KVCoordinator),
		nodes:        make(map[string]*Node),
		emptyNodes:   make(map[*Node]struct{}),
		log:          logger,
	}

	// init engine for cluster.
	if err = engine.Init(c); err != nil {
		return nil, err
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

func (c *Cluster) replaceCoordinator(key string, coordinator KVCoordinator, forceReplace bool) error {
	nodeSet := make(map[*Node]struct{})

	for _, node := range c.nodes {
		if _, exists := nodeSet[node]; exists {
			continue
		}
		nodeSet[node] = struct{}{}

		// get kv entry.
		entry, exists := node.kvs[key]
		if !exists {
			continue
		}
		// no locking for entry cause entire cluster is locked.
		// ensure that existing value is valid for the the coordinator.
		if !coordinator.Validate(entry.KeyValue) {
			if !forceReplace {
				return ErrIncompatibleCoordinator
			}

			// erase entry in case of incompatiable corrdinator.
			delete(node.kvs, key)
		} else {
			entry.coordinator = coordinator // replace
		}
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
