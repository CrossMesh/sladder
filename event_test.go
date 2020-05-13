package sladder

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {
	c, self, err := newTestFakedCluster(nil)
	assert.NotNil(t, c)
	assert.NotNil(t, self)
	assert.NoError(t, err)
	r := c.eventRegistry

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
			wg.Done()
		}()

		wg.Wait()
	})

	r.EventBarrier()
}
