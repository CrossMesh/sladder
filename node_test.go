package sladder

import (
	"fmt"
	"testing"

	"github.com/crossmesh/sladder/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNode(t *testing.T) {
	c, self, err := newTestFakedCluster(&TestRandomNameResolver{}, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, self)

	model1, model2, model3 := &MockKVValidator{}, &MockKVValidator{}, &MockKVValidator{}
	model1.On("Validate", mock.Anything).Return(true)
	model2.On("Validate", mock.Anything).Return(false)
	model3.On("Validate", mock.Anything).Return(true)
	model1.On("Sync", mock.Anything, (*KeyValue)(nil)).Return(true, nil)
	model2.On("Sync", mock.Anything, (*KeyValue)(nil)).Return(true, nil)
	model3.On("Sync", mock.Anything, (*KeyValue)(nil)).Return(true, nil)

	t.Run("test_printable_name", func(t *testing.T) {
		emptyNode := newNode(c)
		assert.Equal(t, "_", emptyNode.PrintableName())

		emptyNode.names = []string{"1"}
		assert.Equal(t, "1", emptyNode.PrintableName())

		emptyNode.names = []string{"1", "2"}
		assert.Equal(t, fmt.Sprintf("%v", emptyNode.names), emptyNode.PrintableName())

		names := self.Names()
		switch len(names) {
		case 1:
			assert.Equal(t, names[0], self.PrintableName())
		case 0:
			assert.Equal(t, "_", self.PrintableName())
		default:
			assert.Equal(t, fmt.Sprintf("%v", names), self.PrintableName())
		}
	})

	t.Run("test_snapshot", func(t *testing.T) {
		kvs := map[string]string{
			"snk1": "v1",
			"snk2": "v2",
			"snk3": "v3",
		}
		for k, v := range kvs {
			assert.NoError(t, c.RegisterKey(k, model1, true, 0))
			assert.Nil(t, self._set(k, v))
		}

		msg := proto.Node{}
		self.ProtobufSnapshot(&msg)
		assert.Equal(t, 3, len(msg.Kvs))
		for _, kv := range msg.Kvs {
			excepted, exist := kvs[kv.Key]
			assert.True(t, exist)
			assert.Equal(t, excepted, kv.Value)
		}

		for _, kv := range self.KeyValueEntries(true) {
			excepted, exist := kvs[kv.Key]
			assert.True(t, exist)
			assert.Equal(t, excepted, kv.Value)
		}
	})
}
