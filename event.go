package sladder

import (
	"sync"

	arbit "github.com/sunmxt/arbiter"
)

// Event is enum type of event in cluster scope.
type Event uint

const (
	// UnknownEvent is undefined event.
	UnknownEvent = Event(0)

	// EmptyNodeJoined tiggered after an empty node(with no name) joined cluster.
	EmptyNodeJoined = Event(1)
	// NodeJoined tiggered after node name is resolved and the node joined cluster.
	NodeJoined = Event(2)
	// NodeRemoved tiggered after a node is removed from cluster.
	NodeRemoved = Event(3)

	// ValueChanged tiggered after a KeyValue value changed.
	ValueChanged = Event(4)
	// KeyDelete tiggered after a KeyValue was removed from a node.
	KeyDelete = Event(5)
	// KeyInsert tiggered after a KeyValue was inserted to a node.
	KeyInsert = Event(6)
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

type eventRegistry struct {
	lock sync.RWMutex

	// event watchers
	eventHandlers             map[*ClusterEventContext]struct{}
	keyEventWatcherIndex      map[string]map[*WatchEventContext]struct{}
	nodeNameEventWatcherIndex map[string]map[*WatchEventContext]struct{}
	nodeEventWatcherIndex     map[*Node]map[*WatchEventContext]struct{}

	// event work queue for seralization.
	sigWork chan func()
}

func newEventRegistry() *eventRegistry {
	return &eventRegistry{
		eventHandlers:             make(map[*ClusterEventContext]struct{}),
		keyEventWatcherIndex:      make(map[string]map[*WatchEventContext]struct{}),
		nodeNameEventWatcherIndex: make(map[string]map[*WatchEventContext]struct{}),
		nodeEventWatcherIndex:     make(map[*Node]map[*WatchEventContext]struct{}),
		sigWork:                   make(chan func()),
	}
}

func (r *eventRegistry) startWorker(arbiter *arbit.Arbiter) {
	type eventWorkList struct {
		work func()
		next *eventWorkList
	}
	workChan := make(chan func())

	var head, tail *eventWorkList
	pool := sync.Pool{
		New: func() interface{} { return &eventWorkList{} },
	}

	// schedule works.
	arbiter.Go(func() {
		var next func()

	Schedule:
		for {
			if next == nil {
				select {
				case <-arbiter.Exit():
					break Schedule
				case ready := <-r.sigWork:
					next = ready
				}

			} else {

				select {
				case <-arbiter.Exit():
					break Schedule

				case ready := <-r.sigWork:
					// enqueue work.
					if head == nil {
						head = pool.Get().(*eventWorkList)
						head.work = ready
						tail = head
					} else {
						tail.next = pool.Get().(*eventWorkList)
						tail = tail.next
						tail.work = ready
					}

				case workChan <- next:
					next = nil
					// dequeue work.
					if head != nil {
						next = head.work
						old := head
						head = old.next
						pool.Put(old)
					}
					if head == nil {
						tail = nil
					}
				}
			}
		}
	})

	// worker
	arbiter.Go(func() {
	Work:
		for {
			select {
			case <-arbiter.Exit():
				break Work

			case work := <-workChan:
				work()
			}
		}
	})
}

func (r *eventRegistry) emitEvent(event Event, node *Node) {
	r.sigWork <- func() {
		// call handlers.
		r.lock.Lock()
		defer r.lock.Unlock()

		for ctx := range r.eventHandlers {
			if ctx.unregistered {
				delete(r.eventHandlers, ctx)
				continue
			}

			ctx.handler(ctx, event, node)

			if ctx.unregistered {
				delete(r.eventHandlers, ctx)
			}
		}
	}
}

// Watch registers event handler of cluster.
func (r *eventRegistry) Watch(handler ClusterEventHandler) *ClusterEventContext {
	if handler == nil {
		return nil
	}

	ctx := &ClusterEventContext{
		handler:      handler,
		unregistered: false,
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	r.eventHandlers[ctx] = struct{}{}

	return ctx
}

// WatchEventHandler handles watch event.
type WatchEventHandler func(*WatchEventContext, KeyValueEventMetadata)

// WatchEventContext contains watch event context.
type WatchEventContext struct {
	opCtx   *OperationContext
	handler WatchEventHandler
}

// KeyValueEventMetadata contains metadata of KeyValue-related event.
type KeyValueEventMetadata interface {
	Key() string
	Node() *Node
	Event() Event
}

type keyValueEvent struct {
	key  string
	node *Node
}

func (e *keyValueEvent) Key() string  { return e.key }
func (e *keyValueEvent) Node() *Node  { return e.node }
func (e *keyValueEvent) Event() Event { return UnknownEvent }

// KeyInsertEventMetadata contains metadata of KeyInsert event.
type KeyInsertEventMetadata interface {
	KeyValueEventMetadata

	Value() string
}

type keyInsertEvent struct {
	keyValueEvent
	value string
}

func (e *keyInsertEvent) Value() string { return e.value }
func (e *keyInsertEvent) Event() Event  { return KeyInsert }

// KeyChangeEventMetadata contains metadata of KeyChange event.
type KeyChangeEventMetadata interface {
	KeyValueEventMetadata

	Old() string
	New() string
}

type keyChangeEvent struct {
	keyValueEvent
	old, new string
}

func (e *keyChangeEvent) Old() string  { return e.old }
func (e *keyChangeEvent) New() string  { return e.new }
func (e *keyChangeEvent) Event() Event { return ValueChanged }

// KeyDeleteEventMetadata contains metadata of KeyDelete event.
type KeyDeleteEventMetadata interface {
	KeyValueEventMetadata

	Value() string
}

type keyDeleteEvent struct {
	keyValueEvent
	value string
}

func (e *keyDeleteEvent) Value() string { return e.value }
func (e *keyDeleteEvent) Event() Event  { return KeyDelete }

func (r *eventRegistry) emitKeyDeletion(node *Node, key, value string) {
	r.emitKVEvent(&keyDeleteEvent{
		keyValueEvent: keyValueEvent{
			key:  key,
			node: node,
		},
		value: value,
	})
}

func (r *eventRegistry) emitKeyInsertion(node *Node, key, value string) {
	r.emitKVEvent(&keyInsertEvent{
		keyValueEvent: keyValueEvent{
			key:  key,
			node: node,
		},
		value: value,
	})
}

func (r *eventRegistry) emitKeyChange(node *Node, key, origin, new string) {
	r.emitKVEvent(&keyChangeEvent{
		keyValueEvent: keyValueEvent{
			key:  key,
			node: node,
		},
		old: origin,
		new: new,
	})
}

func (r *eventRegistry) emitKVEvent(meta KeyValueEventMetadata) {
	r.sigWork <- func() {
		r.lock.RLock()
		defer r.lock.RUnlock()
		for watch := range r.hitWatchContext(meta.Node(), meta.Key()) {
			watch.handler(watch, meta)
		}
	}
}

func (r *eventRegistry) hitWatchContext(node *Node, targetKey string) map[*WatchEventContext]struct{} {

	// pick by *Node
	ctxSet, _ := r.nodeEventWatcherIndex[node]
	emitCtxSet := make(map[*WatchEventContext]struct{}, len(ctxSet))
	for ctx := range ctxSet {
		emitCtxSet[ctx] = struct{}{}
	}
	// pick by node name
	nodeNames := node.names
	for _, name := range nodeNames {
		ctxSet, _ = r.nodeNameEventWatcherIndex[name]
		for ctx := range ctxSet {
			emitCtxSet[ctx] = struct{}{}
		}
	}

	// filter by keys.
filterByKey:
	for ctx := range emitCtxSet {
		if targetKeys := ctx.opCtx.keys; len(targetKeys) > 0 { // has key limit.
			for _, key := range ctx.opCtx.keys {
				if key == targetKey { // hit.
					continue filterByKey
				}
			}
			delete(emitCtxSet, ctx)

		} // else {
		// 		no key filtering....
		// }
	}

	// pick by key.
	keyCtxSet, _ := r.keyEventWatcherIndex[targetKey]
	for ctx := range keyCtxSet {
		// We could just pick those who select the key but didn't select any node.
		// Those who select node and key both are already in emit set.
		if ctx.opCtx.nodes == nil && ctx.opCtx.nodeNames == nil {
			emitCtxSet[ctx] = struct{}{}
		}
	}

	return emitCtxSet
}

func (r *eventRegistry) watchKV(opCtx *OperationContext, handler WatchEventHandler) {
	if handler == nil {
		return
	}

	watchCtx := &WatchEventContext{
		opCtx:   opCtx,
		handler: handler,
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// register watcher.
	for _, key := range opCtx.keys {
		ctxSet, exists := r.keyEventWatcherIndex[key]
		if !exists {
			ctxSet = make(map[*WatchEventContext]struct{})
		}
		ctxSet[watchCtx] = struct{}{}
	}
	for _, nodeName := range opCtx.nodeNames {
		ctxSet, exists := r.nodeNameEventWatcherIndex[nodeName]
		if !exists {
			ctxSet = make(map[*WatchEventContext]struct{})
		}
		ctxSet[watchCtx] = struct{}{}
	}
	for node := range opCtx.nodes {
		ctxSet, exists := r.nodeEventWatcherIndex[node]
		if !exists {
			ctxSet = make(map[*WatchEventContext]struct{})
		}
		ctxSet[watchCtx] = struct{}{}
	}
}
