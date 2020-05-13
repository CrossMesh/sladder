package sladder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperation(t *testing.T) {
	c, self, err := newTestFakedCluster(nil)
	assert.NotNil(t, c)
	assert.NotNil(t, self)
	assert.NoError(t, err)

	o := self.Keys("key1", "key2")
	assert.NotNil(t, o)
	assert.Equal(t, []string{"key1", "key2"}, o.keys)
	assert.Contains(t, o.nodes, self)
	assert.Equal(t, 1, len(o.nodes))

	var n1 *Node
	n1, err = c.NewNode()
	assert.NotNil(t, n1)
	assert.NoError(t, err)

	o = c.Nodes(self, n1)
	assert.NotNil(t, o)
	o = o.Keys("key1", "key2")
	assert.NotNil(t, o)
	assert.Equal(t, []string{"key1", "key2"}, o.keys)
	assert.Contains(t, o.nodes, n1)
	assert.Contains(t, o.nodes, self)
	assert.Equal(t, 2, len(o.nodes))

	o = c.Keys("key3", "key2")
	o = o.Keys("key1", "key2")
	o = o.Nodes(self, n1)
	assert.NotNil(t, o)
	assert.Equal(t, []string{"key1", "key2"}, o.keys)
	assert.Contains(t, o.nodes, n1)
	assert.Contains(t, o.nodes, self)
	assert.Equal(t, 2, len(o.nodes))

	o = o.Nodes(self, "cccp")
	o = o.Nodes(self, n1, "ccc")
	assert.NotNil(t, o)
	assert.Equal(t, []string{"key1", "key2"}, o.keys)
	assert.Contains(t, o.nodes, n1)
	assert.Contains(t, o.nodes, self)
	assert.Equal(t, 2, len(o.nodes))
	assert.Equal(t, 1, len(o.nodeNames))
	assert.Equal(t, "ccc", o.nodeNames[0])
}
