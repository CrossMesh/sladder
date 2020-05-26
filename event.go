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

type eventWorkList struct {
	work func()
	next *eventWorkList
}

type eventRegistry struct {
	lock sync.RWMutex

	arbiter *arbit.Arbiter

	// event watchers
	eventHandlers             map[*ClusterEventContext]struct{}
	keyEventWatcherIndex      map[string]map[*WatchEventContext]struct{}
	nodeNameEventWatcherIndex map[string]map[*WatchEventContext]struct{}
	nodeEventWatcherIndex     map[*Node]map[*WatchEventContext]struct{}

	// event work queue for seralization.
	queueLock            sync.Mutex
	barrierCond          *sync.Cond
	workCond             *sync.Cond
	nextWork             func()
	queueHead, queueTail *eventWorkList
	queuePool            sync.Pool
}

func newEventRegistry(arbiter *arbit.Arbiter) (r *eventRegistry) {
	r = &eventRegistry{
		eventHandlers:             make(map[*ClusterEventContext]struct{}),
		keyEventWatcherIndex:      make(map[string]map[*WatchEventContext]struct{}),
		nodeNameEventWatcherIndex: make(map[string]map[*WatchEventContext]struct{}),
		nodeEventWatcherIndex:     make(map[*Node]map[*WatchEventContext]struct{}),
		queuePool: sync.Pool{
			New: func() interface{} { return &eventWorkList{} },
		},
		arbiter: arbiter,
	}
	r.barrierCond = sync.NewCond(&r.queueLock)
	r.workCond = sync.NewCond(&r.queueLock)
	return
}

func (r *eventRegistry) enqueueWork(work func()) {
	if work == nil {
		return
	}
	if r.nextWork == nil {
		r.nextWork = work
		return
	}
	if r.queueHead == nil {
		r.queueHead = r.queuePool.Get().(*eventWorkList)
		r.queueHead.work = work
		r.queueTail = r.queueHead
	} else {
		r.queueTail.next = r.queuePool.Get().(*eventWorkList)
		r.queueTail = r.queueTail.next
		r.queueTail.work = work
	}
}

func (r *eventRegistry) dequeueWork() (work func()) {
	if r.queueHead == nil {
		return nil
	}
	work = r.queueHead.work
	old := r.queueHead
	r.queueHead = r.queueHead.next
	if r.queueHead == nil {
		r.queueTail = nil
	}

	old.next, old.work = nil, nil // clean
	r.queuePool.Put(old)
	return work
}

func (r *eventRegistry) startWorker() {
	// worker
	r.arbiter.Go(func() {
		r.queueLock.Lock()
		defer r.queueLock.Unlock()

		for r.arbiter.ShouldRun() {
			if r.nextWork == nil {
				r.nextWork = r.dequeueWork() // try dequeue work
				if r.nextWork == nil {
					r.barrierCond.Broadcast()
				}
			}
			if next := r.nextWork; next != nil {
				r.queueLock.Unlock()
				next()
				r.queueLock.Lock()

				r.nextWork = nil
			} else {
				r.workCond.Wait()
			}
		}
	})

	r.arbiter.Go(func() {
		<-r.arbiter.Exit()
		r.queueLock.Lock()
		defer r.queueLock.Unlock()
		r.workCond.Broadcast()
		return
	})
}

// EventBarrier waits until event queue is drained.
func (r *eventRegistry) EventBarrier() {
	r.queueLock.Lock()
	defer r.queueLock.Unlock()
	if r.nextWork != nil || r.queueHead != nil {
		r.barrierCond.Wait()
	}
}

func (r *eventRegistry) emitEvent(event Event, node *Node) {
	r.arbiter.Do(func() {
		r.queueLock.Lock()
		defer r.queueLock.Unlock()

		r.enqueueWork(func() {
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
		})

		r.workCond.Broadcast()
	})
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
	registry *eventRegistry
	opCtx    *OperationContext
	handler  WatchEventHandler
}

// Unregister cancels watch.
func (c *WatchEventContext) Unregister() {
	r := c.registry
	if r == nil {
		return
	}
	r.cancelWatchKV(c)
}

// KeyValueEventMetadata contains metadata of KeyValue-related event.
type KeyValueEventMetadata interface {
	Key() string
	Node() *Node
	Event() Event
	Snapshot() []*KeyValue
}

type keyValueEvent struct {
	key  string
	node *Node
	snap []*KeyValue
}

func (e *keyValueEvent) Key() string           { return e.key }
func (e *keyValueEvent) Node() *Node           { return e.node }
func (e *keyValueEvent) Event() Event          { return UnknownEvent }
func (e *keyValueEvent) Snapshot() []*KeyValue { return e.snap }

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

func (r *eventRegistry) emitKeyDeletion(node *Node, key, value string, snap []*KeyValue) {
	r.emitKVEvent(&keyDeleteEvent{
		keyValueEvent: keyValueEvent{
			key:  key,
			node: node,
			snap: snap,
		},
		value: value,
	})
}

func (r *eventRegistry) emitKeyInsertion(node *Node, key, value string, snap []*KeyValue) {
	r.emitKVEvent(&keyInsertEvent{
		keyValueEvent: keyValueEvent{
			key:  key,
			node: node,
			snap: snap,
		},
		value: value,
	})
}

func (r *eventRegistry) emitKeyChange(node *Node, key, origin, new string, snap []*KeyValue) {
	r.emitKVEvent(&keyChangeEvent{
		keyValueEvent: keyValueEvent{
			key:  key,
			node: node,
			snap: snap,
		},
		old: origin,
		new: new,
	})
}

func (r *eventRegistry) emitKVEvent(meta KeyValueEventMetadata) {
	r.arbiter.Do(func() {
		r.queueLock.Lock()
		defer r.queueLock.Unlock()
		r.enqueueWork(func() {
			r.lock.RLock()
			defer r.lock.RUnlock()
			for watch := range r.hitWatchContext(meta.Node(), meta.Key()) {
				watch.handler(watch, meta)
			}
		})
		r.workCond.Broadcast()
	})
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
		if len(ctx.opCtx.nodes) < 1 && len(ctx.opCtx.nodeNames) < 1 {
			emitCtxSet[ctx] = struct{}{}
		}
	}

	return emitCtxSet
}

func (r *eventRegistry) cancelWatchKV(watchCtx *WatchEventContext) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// unregister watcher.
	for _, key := range watchCtx.opCtx.keys {
		ctxSet, exists := r.keyEventWatcherIndex[key]
		if !exists {
			continue
		}
		delete(ctxSet, watchCtx)
		if len(ctxSet) < 1 {
			delete(r.keyEventWatcherIndex, key)
		}
	}
	for _, nodeName := range watchCtx.opCtx.nodeNames {
		ctxSet, exists := r.nodeNameEventWatcherIndex[nodeName]
		if !exists {
			continue
		}
		delete(ctxSet, watchCtx)
		if len(ctxSet) < 1 {
			delete(r.nodeNameEventWatcherIndex, nodeName)
		}
	}
	for node := range watchCtx.opCtx.nodes {
		ctxSet, exists := r.nodeEventWatcherIndex[node]
		if !exists {
			continue
		}
		delete(ctxSet, watchCtx)
		if len(ctxSet) < 1 {
			delete(r.nodeEventWatcherIndex, node)
		}
	}
}

func (r *eventRegistry) watchKV(opCtx *OperationContext, handler WatchEventHandler) (watchCtx *WatchEventContext) {
	if handler == nil {
		return
	}

	watchCtx = &WatchEventContext{
		registry: r,
		opCtx:    opCtx,
		handler:  handler,
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// register watcher.
	for _, key := range opCtx.keys {
		ctxSet, exists := r.keyEventWatcherIndex[key]
		if !exists {
			ctxSet = make(map[*WatchEventContext]struct{})
			r.keyEventWatcherIndex[key] = ctxSet
		}
		ctxSet[watchCtx] = struct{}{}
	}
	for _, nodeName := range opCtx.nodeNames {
		ctxSet, exists := r.nodeNameEventWatcherIndex[nodeName]
		if !exists {
			ctxSet = make(map[*WatchEventContext]struct{})
			r.nodeNameEventWatcherIndex[nodeName] = ctxSet
		}
		ctxSet[watchCtx] = struct{}{}
	}
	for node := range opCtx.nodes {
		ctxSet, exists := r.nodeEventWatcherIndex[node]
		if !exists {
			ctxSet = make(map[*WatchEventContext]struct{})
			r.nodeEventWatcherIndex[node] = ctxSet
		}
		ctxSet[watchCtx] = struct{}{}
	}

	return
}
