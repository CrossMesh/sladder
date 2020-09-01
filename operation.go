package sladder

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
