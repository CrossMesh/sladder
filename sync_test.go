package sladder

import (
	"errors"
	"testing"

	"github.com/crossmesh/sladder/proto"
	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	t.Run("test_node", func(t *testing.T) {
		c, self, err := newTestFakedCluster(nil, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, self)

		m := &MockKVTransactionValidator{}
		assert.NoError(t, c.RegisterKey("key1", m, true, 0))
		assert.NoError(t, c.RegisterKey("key2", m, true, 0))
		assert.NoError(t, c.RegisterKey("key3", m, true, 0))

		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			assert.NoError(t, tx.MergeNodeSnapshot(nil, nil, true, true, true))
			return false
		}))

		// normal sync.
		e := self.get("key1")
		assert.Nil(t, e)
		e = self.get("key2")
		assert.Nil(t, e)
		pb1 := &proto.Node{
			Kvs: []*proto.Node_KeyValue{
				{Key: "key1", Value: "v1"},
				{Key: "key2", Value: "v2"},
				{Key: "key3", Value: "v3"},
			},
		}
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			assert.NoError(t, tx.MergeNodeSnapshot(self, pb1, true, true, true))
			return true
		}))

		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v1", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key3")
		assert.NotNil(t, e)
		assert.Equal(t, "key3", e.Key)
		assert.Equal(t, "v3", e.Value)

		// key changes.
		pb1.Kvs[0].Value = "v2"
		pb1.Kvs[1].Value = "v3"
		pb1.Kvs[2].Value = "v4"
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			assert.NoError(t, tx.MergeNodeSnapshot(self, pb1, true, true, true))
			return true
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v3", e.Value)
		e = self.get("key3")
		assert.NotNil(t, e)
		assert.Equal(t, "key3", e.Key)
		assert.Equal(t, "v4", e.Value)

		// missing validator.
		pb1.Kvs = append(pb1.Kvs, &proto.Node_KeyValue{Key: "key4", Value: "v10"})
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			assert.Equal(t, ErrValidatorMissing, tx.MergeNodeSnapshot(self, pb1, true, true, true))
			return false
		}))
		e = self.get("key4")
		assert.Nil(t, e)
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v3", e.Value)
		e = self.get("key3")
		assert.NotNil(t, e)
		assert.Equal(t, "key3", e.Key)
		assert.Equal(t, "v4", e.Value)

		// key deletion.
		pb1.Kvs[1] = pb1.Kvs[2]
		pb1.Kvs = pb1.Kvs[:2]
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			assert.NoError(t, tx.MergeNodeSnapshot(self, pb1, true, true, true))
			return true
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.Nil(t, e)
		e = self.get("key3")
		assert.NotNil(t, e)
		assert.Equal(t, "key3", e.Key)
		assert.Equal(t, "v4", e.Value)

		// fail validator sync.
		pb1.Kvs = pb1.Kvs[:1]
		testError := errors.New("testerr")
		m.AddFailKey("key1", testError)
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			assert.Equal(t, testError, tx.MergeNodeSnapshot(self, pb1, true, true, true))
			return false
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.Nil(t, e)
		e = self.get("key3")
		assert.NotNil(t, e)
		assert.Equal(t, "key3", e.Key)
		assert.Equal(t, "v4", e.Value)

	})

	t.Run("test_cluster", func(t *testing.T) {
		mnr := &MockNodeNameKVResolver{}
		mnr.UseKeyAsID("id1", "id2", "id3")

		c, self, err := newTestFakedCluster(mnr, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, self)

		m := &MockKVTransactionValidator{}
		assert.NoError(t, c.RegisterKey("id1", m, true, 0))
		assert.NoError(t, c.RegisterKey("id2", m, true, 0))
		assert.NoError(t, c.RegisterKey("id3", m, true, 0))
		assert.NoError(t, c.RegisterKey("key1", m, true, 0))

		cpb := &proto.Cluster{
			Nodes: []*proto.Node{
				{
					Kvs: []*proto.Node_KeyValue{
						{Key: "id1", Value: "n1-1"},
						{Key: "id2", Value: "n1-2"},
						{Key: "id3", Value: "n1-3"},
						{Key: "key1", Value: "n1-4"},
					},
				},
				{
					Kvs: []*proto.Node_KeyValue{
						{Key: "id1", Value: "n2-1"},
						{Key: "id2", Value: "n2-2"},
						{Key: "key1", Value: "n2-3"},
					},
				},
			},
		}
		self._set("id1", "n0-1")
		self._set("id2", "n0-2")
		self._set("id3", "n0-3")

		t.Run("test_resolve_from_pb", func(t *testing.T) {
			names, err := c.resolveNodeNameFromProtobuf(cpb.Nodes[0].Kvs)
			assert.NoError(t, err)
			assert.Equal(t, 3, len(names))
			assert.Contains(t, names, "n1-1")
			assert.Contains(t, names, "n1-2")
			assert.Contains(t, names, "n1-3")

			names, err = c.resolveNodeNameFromProtobuf(cpb.Nodes[1].Kvs)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(names))
			assert.Contains(t, names, "n2-1")
			assert.Contains(t, names, "n2-2")
		})
	})
}
