package sladder

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {
	receiveMeta := func(ctx context.Context, cs ...chan KeyValueEventMetadata) (metas []KeyValueEventMetadata) {
		if len(cs) < 1 {
			return nil
		}

		var wg sync.WaitGroup
		metas = make([]KeyValueEventMetadata, len(cs))
		for idx, c := range cs {
			wg.Add(1)
			go func(idx int, c chan KeyValueEventMetadata) {
				defer wg.Done()
				select {
				case <-ctx.Done():
					assert.Fail(t, "event receiving context exceeded.")
				case meta := <-c:
					metas[idx] = meta
				}
			}(idx, c)
		}
		wg.Wait()
		return
	}

	noMoreMeta := func(cs ...chan KeyValueEventMetadata) (meta KeyValueEventMetadata) {
		for _, c := range cs {
			select {
			case meta = <-c:
				assert.Fail(t, "too many event.")
			default:
			}
		}
		return
	}

	c, self, err := newTestFakedCluster(&TestRandomNameResolver{
		NumOfNames: 1,
	}, nil, nil)
	assert.NotNil(t, c)
	assert.NotNil(t, self)
	assert.NoError(t, err)
	r := c.eventRegistry

	r.EventBarrier()

	t.Run("test_cluster_event_cancel_watch", func(t *testing.T) {
		called := false
		handler := func(ctx *ClusterEventContext, e Event, n *Node) { called = true }
		ctx := r.Watch(handler)
		assert.NotNil(t, ctx)
		r.emitEvent(EmptyNodeJoined, self)
		r.EventBarrier()
		assert.True(t, called)

		ctx.Unregister()
		called = false
		r.emitEvent(EmptyNodeJoined, self)
		r.EventBarrier()
		assert.False(t, called)
	})

	r.EventBarrier()

	t.Run("test_cluster_event_inner_cancel_watch", func(t *testing.T) {
		called := false
		handler := func(ctx *ClusterEventContext, e Event, n *Node) {
			called = true
			ctx.Unregister()
		}
		ctx := r.Watch(handler)
		assert.NotNil(t, ctx)
		r.emitEvent(EmptyNodeJoined, self)
		r.EventBarrier()
		assert.True(t, called)

		called = false
		r.emitEvent(EmptyNodeJoined, self)
		r.EventBarrier()
		assert.False(t, called)
	})

	r.EventBarrier()

	t.Run("test_cluster_event", func(t *testing.T) {
		event, node := make(chan Event), make(chan *Node)
		handler := func(ctx *ClusterEventContext, e Event, n *Node) {
			event <- e
			node <- n
		}
		ctx := r.Watch(handler)
		assert.NotNil(t, ctx)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			r.emitEvent(EmptyNodeJoined, self)
			r.emitEvent(NodeJoined, self)
			r.emitEvent(NodeRemoved, self)
			r.emitEvent(EmptyNodeJoined, self)
			r.emitEvent(NodeJoined, self)
			r.emitEvent(NodeRemoved, self)
			wg.Done()
		}()

		wg.Add(1)
		// event order should be preserved.
		go func() {
			assert.Equal(t, EmptyNodeJoined, <-event)
			assert.Equal(t, self, <-node)
			assert.Equal(t, NodeJoined, <-event)
			assert.Equal(t, self, <-node)
			assert.Equal(t, NodeRemoved, <-event)
			assert.Equal(t, self, <-node)
			assert.Equal(t, EmptyNodeJoined, <-event)
			assert.Equal(t, self, <-node)
			assert.Equal(t, NodeJoined, <-event)
			assert.Equal(t, self, <-node)
			assert.Equal(t, NodeRemoved, <-event)
			assert.Equal(t, self, <-node)
			ctx.Unregister()
			wg.Done()
		}()

		wg.Wait()
	})

	r.EventBarrier()

	t.Run("test_kv_watch_register", func(t *testing.T) {
		called := false
		ctx := c.Keys("key1").Watch(func(ctx *WatchEventContext, meta KeyValueEventMetadata) {
			called = true
		})
		r.emitKeyInsertion(self, "key1", "2", self.KeyValueEntries(true))
		r.EventBarrier()
		assert.True(t, called)

		ctx.Unregister()
		called = false
		r.emitKeyInsertion(self, "key1", "2", self.KeyValueEntries(true))
		r.EventBarrier()
		assert.False(t, called)
	})

	r.EventBarrier()

	t.Run("test_kv_watch_kv_event", func(t *testing.T) {
		n1, err := c.NewNode()
		assert.NoError(t, err)
		assert.NotNil(t, n1)

		// only keys.
		onlyKey := make(chan KeyValueEventMetadata)
		ctx1 := c.Keys("key1").Watch(func(ctx *WatchEventContext, meta KeyValueEventMetadata) {
			onlyKey <- meta
		})
		assert.NotNil(t, ctx1)
		// only node ptr.
		onlyNode := make(chan KeyValueEventMetadata)
		ctx2 := c.Nodes(self).Watch(func(ctx *WatchEventContext, meta KeyValueEventMetadata) {
			onlyNode <- meta
		})
		assert.NotNil(t, ctx2)
		// only node name.
		names := make([]interface{}, 1)
		for _, name := range self.Names() {
			names = append(names, name)
		}
		onlyNodeName := make(chan KeyValueEventMetadata)
		ctx3 := c.Nodes(names...).Watch(func(ctx *WatchEventContext, meta KeyValueEventMetadata) {
			onlyNodeName <- meta
		})
		assert.NotNil(t, ctx3)
		// key and node ptr.
		keyAndNode := make(chan KeyValueEventMetadata)
		ctx4 := c.Nodes(self).Keys("key1").Watch(func(ctx *WatchEventContext, meta KeyValueEventMetadata) {
			keyAndNode <- meta
		})
		assert.NotNil(t, ctx4)
		// key and node names.
		keyAndNodeName := make(chan KeyValueEventMetadata)
		ctx5 := c.Nodes(names...).Keys("key1").Watch(func(ctx *WatchEventContext, meta KeyValueEventMetadata) {
			keyAndNodeName <- meta
		})
		assert.NotNil(t, ctx5)

		r.EventBarrier()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			// insert
			r.emitKeyInsertion(self, "key1", "2", self.KeyValueEntries(true))
			r.emitKeyInsertion(n1, "key1", "4", n1.KeyValueEntries(true))
			r.emitKeyInsertion(self, "key2", "3", self.KeyValueEntries(true))
			r.emitKeyInsertion(n1, "key2", "1", n1.KeyValueEntries(true))

			// change
			r.emitKeyChange(self, "key1", "2", "3", self.KeyValueEntries(true))
			r.emitKeyChange(self, "key2", "3", "1", self.KeyValueEntries(true))
			r.emitKeyChange(n1, "key1", "2", "3", self.KeyValueEntries(true))
			r.emitKeyChange(n1, "key2", "1", "5", self.KeyValueEntries(true))

			// delete
			r.emitKeyDeletion(self, "key1", "2", self.KeyValueEntries(true))
			r.emitKeyDeletion(self, "key2", "3", self.KeyValueEntries(true))
			r.EventBarrier()

			wg.Done()
		}()

		wg.Add(1)
		go func() {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
			defer cancel()

			// insert
			for _, meta := range receiveMeta(ctx, onlyKey, onlyNode, onlyNodeName, keyAndNode, keyAndNodeName) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, KeyInsert, meta.Event())
				assert.Equal(t, "key1", meta.Key())
				im := meta.(KeyInsertEventMetadata)
				assert.Equal(t, "2", im.Value())
			}

			for _, meta := range receiveMeta(ctx, onlyKey) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, KeyInsert, meta.Event())
				assert.Equal(t, "key1", meta.Key())
				im := meta.(KeyInsertEventMetadata)
				assert.Equal(t, "4", im.Value())
			}

			for _, meta := range receiveMeta(ctx, onlyNode, onlyNodeName) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, KeyInsert, meta.Event())
				assert.Equal(t, "key2", meta.Key())
				im := meta.(KeyInsertEventMetadata)
				assert.Equal(t, "3", im.Value())
			}

			// change
			for _, meta := range receiveMeta(ctx, onlyKey, onlyNode, onlyNodeName, keyAndNode, keyAndNodeName) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, ValueChanged, meta.Event())
				assert.Equal(t, "key1", meta.Key())
				im := meta.(KeyChangeEventMetadata)
				assert.Equal(t, "2", im.Old())
				assert.Equal(t, "3", im.New())
			}

			for _, meta := range receiveMeta(ctx, onlyNode, onlyNodeName) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, ValueChanged, meta.Event())
				assert.Equal(t, "key2", meta.Key())
				im := meta.(KeyChangeEventMetadata)
				assert.Equal(t, "3", im.Old())
				assert.Equal(t, "1", im.New())
			}

			for _, meta := range receiveMeta(ctx, onlyKey) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, ValueChanged, meta.Event())
				assert.Equal(t, "key1", meta.Key())
				im := meta.(KeyChangeEventMetadata)
				assert.Equal(t, "2", im.Old())
				assert.Equal(t, "3", im.New())
			}

			for _, meta := range receiveMeta(ctx, onlyKey, onlyNode, onlyNodeName, keyAndNode, keyAndNodeName) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, KeyDelete, meta.Event())
				assert.Equal(t, "key1", meta.Key())
				im := meta.(KeyDeleteEventMetadata)
				assert.Equal(t, "2", im.Value())
			}

			for _, meta := range receiveMeta(ctx, onlyNode, onlyNodeName) {
				assert.NotNil(t, meta)
				if meta == nil {
					continue
				}
				assert.Equal(t, KeyDelete, meta.Event())
				assert.Equal(t, "key2", meta.Key())
				im := meta.(KeyDeleteEventMetadata)
				assert.Equal(t, "3", im.Value())
			}

			// no more event.
			noMoreMeta(onlyKey, onlyNode, onlyNodeName, keyAndNode, keyAndNodeName)

			wg.Done()
		}()

		wg.Wait()
		ctx1.Unregister()
		ctx2.Unregister()
		ctx3.Unregister()
		ctx4.Unregister()
		ctx5.Unregister()

		assert.True(t, c.RemoveNode(n1))
	})
}
