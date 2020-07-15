package sladder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"github.com/sunmxt/sladder/proto"
)

type MockNodeNameKVResolver struct {
	MockNodeNameResolver

	IDKeys map[string]struct{}
}

func (r *MockNodeNameKVResolver) UseKeyAsID(keys ...string) {
	if r.IDKeys == nil {
		r.IDKeys = make(map[string]struct{})
	}
	for _, key := range keys {
		r.IDKeys[key] = struct{}{}
	}
}

func (r *MockNodeNameKVResolver) Keys() (keys []string) {
	if r.IDKeys == nil {
		return r.MockNodeNameResolver.Keys()
	}
	for key := range r.IDKeys {
		keys = append(keys, key)
	}
	return
}

func (r *MockNodeNameKVResolver) Resolve(kvs ...*KeyValue) (ids []string, err error) {
	if r.IDKeys == nil {
		return r.MockNodeNameResolver.Resolve(kvs...)
	}
	for _, kv := range kvs {
		if _, in := r.IDKeys[kv.Key]; in {
			ids = append(ids, kv.Value)
		}
	}
	return
}

func newTestFakedCluster(r NodeNameResolver, ei EngineInstance, logger Logger) (*Cluster, *Node, error) {
	if r == nil {
		mnr := &TestRandomNameResolver{NumOfNames: 1}
		r = mnr
	}
	if ei == nil {
		e := &MockEngineInstance{}
		e.Mock.On("Init", mock.Anything).Return(error(nil))
		e.Mock.On("Close").Return(error(nil))
		ei = e
	}

	return NewClusterWithNameResolver(ei, r, logger)
}

func TestCluster(t *testing.T) {
	t.Run("cluster_new_normal", func(t *testing.T) {
		c, self, err := newTestFakedCluster(nil, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, self)

		assert.Equal(t, self, c.Self())
	})

	t.Run("cluster_quit", func(t *testing.T) {
		ei, nr := &MockEngineInstance{}, &TestRandomNameResolver{}
		ei.On("Close").Return(nil)
		ei.Mock.On("Init", mock.Anything).Return(error(nil))

		c, self, err := NewClusterWithNameResolver(ei, nr, nil)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, self)

		c.Quit()
		ei.AssertCalled(t, "Close")
	})

	t.Run("cluster_new_engine_init_error", func(t *testing.T) {
		ei, mnr := &MockEngineInstance{}, &MockNodeNameResolver{}

		ei.Mock.On("Init", mock.Anything).Return(errors.New("init err"))
		ei.Mock.On("Close").Return(error(nil))
		mnr.On("Keys").Return([]string{"id"})
		mnr.On("Resolve", mock.Anything).Return([]string{"id1"}, error(nil))

		c, self, err := NewClusterWithNameResolver(ei, mnr, nil)
		assert.Error(t, err)
		assert.Nil(t, c)
		assert.Nil(t, self)
	})

	t.Run("cluster_new_name_resolve_error", func(t *testing.T) {
		ei, mnr := &MockEngineInstance{}, &MockNodeNameResolver{}

		ei.Mock.On("Init", mock.Anything).Return(error(nil))
		ei.Mock.On("Close").Return(error(nil))
		mnr.On("Keys").Return([]string{"id"})
		mnr.On("Resolve", mock.Anything).Return(nil, errors.New("resolve error"))

		c, self, err := NewClusterWithNameResolver(ei, mnr, nil)
		assert.Error(t, err)
		assert.Nil(t, c)
		assert.Nil(t, self)
	})

	t.Run("cluster_new_engine_close_error", func(t *testing.T) {
		ei, mnr := &MockEngineInstance{}, &MockNodeNameResolver{}

		ei.Mock.On("Init", mock.Anything).Return(error(nil))
		ei.Mock.On("Close").Return(errors.New("engine close error"))
		mnr.On("Keys").Return([]string{"id"})
		mnr.On("Resolve", mock.Anything).Return(nil, errors.New("resolve error"))

		assert.Panics(t, func() {
			c, self, err := NewClusterWithNameResolver(ei, mnr, nil)
			assert.Error(t, err)
			assert.Nil(t, c)
			assert.Nil(t, self)
		})
	})

	t.Run("cluster_new_nil_resolver", func(t *testing.T) {
		ei := &MockEngineInstance{}

		c, self, err := NewClusterWithNameResolver(ei, nil, nil)
		assert.EqualError(t, err, ErrMissingNameResolver.Error())
		assert.Nil(t, c)
		assert.Nil(t, self)
	})

	t.Run("test_name_resolve", func(t *testing.T) {
		mnr := &MockNodeNameKVResolver{}
		mnr.UseKeyAsID("id1", "id2", "id3")

		m := &MockKVValidator{}
		m.On("Validate", mock.Anything).Return(true)
		m.On("Sync", mock.Anything, (*KeyValue)(nil)).Return(true, nil)

		c, self, err := newTestFakedCluster(mnr, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, self)
		assert.NotNil(t, c)
		assert.NoError(t, c.RegisterKey("id1", m, false, 0))
		assert.NoError(t, c.RegisterKey("id2", m, false, 0))
		assert.NoError(t, c.RegisterKey("id3", m, false, 0))
		assert.Equal(t, 0, len(self.Names()))

		var n1 *Node
		n1, err = c.NewNode()
		assert.NoError(t, err)
		assert.NotNil(t, n1)

		// add ids.
		self._set("id1", "ea309")
		self._set("id2", "ea307")
		c.EventBarrier()
		assert.Equal(t, 2, len(self.Names()))
		assert.Contains(t, self.names, "ea309")
		assert.Contains(t, self.names, "ea307")
		assert.NotNil(t, c.GetNode("ea309"))
		assert.NotNil(t, c.GetNode("ea307"))

		// id changes.
		self._set("id1", "ea388")
		self._set("id2", "ea381")
		self._set("id3", "ea389")
		c.EventBarrier()
		assert.Equal(t, 3, len(self.Names()))
		assert.Contains(t, self.names, "ea388")
		assert.Contains(t, self.names, "ea381")
		assert.Contains(t, self.names, "ea389")
		assert.NotNil(t, c.GetNode("ea389"))
		assert.NotNil(t, c.GetNode("ea388"))
		assert.NotNil(t, c.GetNode("ea381"))

		// reject duplated name.
		n1._set("id1", "ea389")
		n1._set("id2", "ea008")
		c.EventBarrier()
		assert.Equal(t, 1, len(n1.Names()))
		assert.Contains(t, n1.names, "ea008")
		assert.Equal(t, self, c.GetNode("ea389"))
		assert.Equal(t, n1, c.GetNode("ea008"))

		// id delete.
		assert.NoError(t, c.RegisterKey("id3", nil, false, 0))
		c.EventBarrier()
		assert.Equal(t, 2, len(self.Names()))
		assert.Contains(t, self.names, "ea388")
		assert.Contains(t, self.names, "ea381")
		assert.Nil(t, c.GetNode("ea389"))
		assert.NotNil(t, c.GetNode("ea388"))
		assert.NotNil(t, c.GetNode("ea381"))

		self.Delete("id1")
		self.Delete("id2")
		n1.Delete("id1")
		n1.Delete("id2")
		c.EventBarrier()
		assert.Equal(t, 0, len(self.Names()))
		assert.Nil(t, c.GetNode("ea389"))
		assert.Nil(t, c.GetNode("ea388"))
		assert.Nil(t, c.GetNode("ea381"))
		assert.True(t, c.ContainNodes(self))
		assert.False(t, c.ContainNodes(n1))
	})

	c, self, err := newTestFakedCluster(&TestRandomNameResolver{
		NumOfNames: 2,
	}, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, self)
	model1, model2, model3 := &MockKVValidator{}, &MockKVValidator{}, &MockKVValidator{}
	model1.On("Validate", mock.Anything).Return(true)
	model2.On("Validate", mock.Anything).Return(false)
	model3.On("Validate", mock.Anything).Return(true)

	t.Run("test_register_key", func(t *testing.T) {
		assert.NoError(t, c.RegisterKey("key1", nil, false, 0)) // dummy
		assert.Error(t, ErrValidatorMissing, self._set("key1", "v1"))

		assert.NoError(t, c.RegisterKey("key1", model1, false, 0))
		assert.Nil(t, self._set("key1", "v1"))
		assert.Nil(t, self._set("key1", "v2"))

		// common test.
		entry := self.getEntry("key1")
		assert.NotNil(t, entry)
		assert.Equal(t, model1, entry.validator)

		assert.NoError(t, c.RegisterKey("key1", model3, false, 0))
		entry = self.getEntry("key1")
		assert.NotNil(t, entry)
		assert.Equal(t, model3, entry.validator)

		assert.Equal(t, ErrIncompatibleValidator, c.RegisterKey("key1", model2, false, 0))
		entry = self.getEntry("key1")
		assert.NotNil(t, entry)
		assert.Equal(t, model3, entry.validator)

		assert.NoError(t, c.RegisterKey("key1", model2, true, 0))
		entry = self.getEntry("key1")
		assert.Nil(t, entry)

		// removal.
		assert.NoError(t, c.RegisterKey("key1", model1, true, 0))
		assert.Nil(t, self._set("key1", "v1"))
		entry = self.getEntry("key1")
		assert.NotNil(t, entry)
		assert.Equal(t, model1, entry.validator)
		assert.NoError(t, c.RegisterKey("key1", nil, false, 0))
		entry = self.getEntry("key1")
		assert.Nil(t, entry)
	})

	t.Run("test_node_op", func(t *testing.T) {
		n, err := c.NewNode()
		assert.NotNil(t, n)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(n.Names()), 1)

		// get node.
		for _, name := range n.Names() {
			assert.Equal(t, n, c.GetNode(name))
		}

		// iterator over nodes. (only one node except myself)
		c.RangeNodes(nil, true, true)
		c.RangeNodes(func(node *Node) bool {
			assert.Equal(t, n, node)
			return true
		}, true, true)
		// iterator over nodes. (only two nodes)
		c.RangeNodes(func(node *Node) bool {
			assert.True(t, node == n || node == self)
			return true
		}, false, true)

		// removal
		assert.False(t, c.RemoveNode(nil))
		assert.True(t, c.RemoveNode(n))
		for _, name := range n.Names() {
			assert.Nil(t, c.GetNode(name))
		}
		c.RangeNodes(func(node *Node) bool {
			assert.Fail(t, "no node should exist")
			return true
		}, true, true)
		c.RangeNodes(func(node *Node) bool {
			assert.True(t, node == self)
			return true
		}, false, true)
	})

	t.Run("test_register_key_atomic", func(t *testing.T) {
		n, err := c.NewNode()
		assert.NoError(t, err)
		assert.NotNil(t, n)

		model := &MockKVValidator{}
		model.On("Validate", mock.MatchedBy(func(entry KeyValue) bool {
			return entry.Value == "v1"
		})).Return(true)
		model.On("Validate", mock.MatchedBy(func(entry KeyValue) bool {
			return entry.Value == "v2"
		})).Return(false)

		assert.NoError(t, c.RegisterKey("key1", model1, false, 0))
		assert.Nil(t, self._set("key1", "v1"))
		assert.Nil(t, n._set("key1", "v2"))
		entry1, entry2 := self.get("key1"), n.get("key1")
		assert.NotNil(t, entry1)
		assert.NotNil(t, entry2)
		assert.Equal(t, "key1", entry1.Key)
		assert.Equal(t, "key1", entry2.Key)
		assert.Equal(t, "v1", entry1.Value)
		assert.Equal(t, "v2", entry2.Value)

		// replace all or do nothing.
		assert.Error(t, ErrIncompatibleValidator, c.RegisterKey("key1", model, false, 0))
		entry1, entry2 = self.get("key1"), n.get("key1")
		assert.NotNil(t, entry1)
		assert.NotNil(t, entry2)
		assert.Equal(t, "key1", entry1.Key)
		assert.Equal(t, "key1", entry2.Key)
		assert.Equal(t, "v1", entry1.Value)
		assert.Equal(t, "v2", entry2.Value)

		assert.NoError(t, c.RegisterKey("key1", model, true, 0))
		entry1, entry2 = self.get("key1"), n.get("key1")
		assert.NotNil(t, entry1)
		assert.Nil(t, entry2)
		assert.Equal(t, "key1", entry1.Key)
		assert.Equal(t, "v1", entry1.Value)

		// cleaning.
		assert.NoError(t, c.RegisterKey("key1", nil, false, 0))
		entry1, entry2 = self.get("key1"), n.get("key1")
		assert.Nil(t, entry1)
		assert.Nil(t, entry2)
		assert.True(t, c.RemoveNode(n))
	})

	t.Run("test_snapshot", func(t *testing.T) {
		var (
			n1, n2 *Node
			err    error
		)

		n1, err = c.NewNode()
		assert.NotNil(t, n1)
		assert.NoError(t, err)
		n2, err = c.NewNode()
		assert.NotNil(t, n2)
		assert.NoError(t, err)

		assert.NoError(t, c.RegisterKey("key1", model1, false, 0))
		assert.NoError(t, c.RegisterKey("key2", model1, false, 0))
		assert.NoError(t, c.RegisterKey("key3", model1, false, 0))
		assert.NoError(t, c.RegisterKey("id", model3, false, 0))

		kvsSelf := map[string]string{
			"key1": "v1",
			"key2": "v2",
			"key3": "v3",
			"id":   self.PrintableName(),
		}
		kvs1 := map[string]string{
			"key1": "v1",
			"key2": "v2",
			"id":   n1.PrintableName(),
		}
		kvs2 := map[string]string{
			"key1": "v1",
			"key3": "v3",
			"id":   n2.PrintableName(),
		}
		nodeToKV := map[*Node]map[string]string{
			self: kvsSelf,
			n1:   kvs1,
			n2:   kvs2,
		}
		msgNodeIDToNode := map[string]*Node{
			kvsSelf["id"]: self,
			kvs1["id"]:    n1,
			kvs2["id"]:    n2,
		}
		for node, kvs := range nodeToKV {
			for key, value := range kvs {
				assert.NoError(t, node._set(key, value))
			}
		}

		var msg proto.Cluster
		c.ProtobufSnapshot(nil, nil)
		c.ProtobufSnapshot(&msg, nil)
		c.ProtobufSnapshot(&msg, nil)
		assert.Equal(t, 3, len(msg.Nodes))
		for _, node := range msg.Nodes {
			guess := (*Node)(nil)

			for _, kv := range node.Kvs {
				if kv.Key == "id" {
					guess, _ = msgNodeIDToNode[kv.Value]
					assert.NotNil(t, guess)
					break
				}
			}
			assert.NotNil(t, guess)
			kvMap, existKVs := nodeToKV[guess]
			assert.True(t, existKVs)
			assert.Equal(t, len(kvMap), len(node.Kvs))
			for _, kv := range node.Kvs {
				excepted, exist := kvMap[kv.Key]
				assert.True(t, exist)
				assert.Equal(t, excepted, kv.Value)
			}
		}

		// cleaning.
		assert.True(t, c.RemoveNode(n1))
		assert.True(t, c.RemoveNode(n2))
		assert.NoError(t, c.RegisterKey("key1", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key2", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key3", nil, true, 0))
	})

	t.Run("test_most_possiable", func(t *testing.T) {
		var (
			n1, n2, n3 *Node
			err        error
		)
		n1, err = c.NewNode()
		assert.NotNil(t, n1)
		assert.NoError(t, err)
		n2, err = c.NewNode()
		assert.NotNil(t, n2)
		assert.NoError(t, err)
		n3, err = c.NewNode()
		assert.NotNil(t, n3)
		assert.NoError(t, err)

		var lookup []string
		names := n1.Names()
		assert.GreaterOrEqual(t, len(names), 2)
		lookup = append(lookup, names...)
		names = n2.Names()
		assert.GreaterOrEqual(t, len(names), 1)
		lookup = append(lookup, names[0], "faked1", "faked2")
		t.Log("lookup: ", lookup)
		assert.Equal(t, n1, c.MostPossibleNode(lookup))
		assert.Nil(t, c.MostPossibleNode(nil))

		// cleaning.
		assert.True(t, c.RemoveNode(n1))
		assert.True(t, c.RemoveNode(n2))
		assert.True(t, c.RemoveNode(n3))
	})
}
