package sladder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockKVTransaction struct {
	updated  bool
	old, new string
}

func (m *MockKVTransaction) Set(key string)             { m.updated, m.new = true, key }
func (m *MockKVTransaction) After() string              { return m.new }
func (m *MockKVTransaction) Updated() bool              { return m.updated }
func (m *MockKVTransaction) Before() string             { return m.old }
func (m *MockKVTransaction) SetRawValue(x string) error { m.updated, m.new = x != m.old, x; return nil }

type MockKVTransactionValidator struct {
	SyncFailKeyMap map[string]error
}

func (v *MockKVTransactionValidator) AddFailKey(key string, err error) {
	if v.SyncFailKeyMap == nil {
		v.SyncFailKeyMap = make(map[string]error)
	}
	v.SyncFailKeyMap[key] = err
}

func (v *MockKVTransactionValidator) DelateFailKey(key string) {
	if v.SyncFailKeyMap == nil {
		return
	}
	delete(v.SyncFailKeyMap, key)
}

func (v *MockKVTransactionValidator) Sync(e *KeyValue, kv *KeyValue) (bool, error) {
	if e == nil {
		return false, nil
	}
	if v.SyncFailKeyMap != nil {
		if err, fail := v.SyncFailKeyMap[e.Key]; fail {
			return false, err
		}
	}
	if kv != nil {
		e.Key = kv.Key
		e.Value = kv.Value
	}
	return true, nil
}
func (v *MockKVTransactionValidator) Validate(kv KeyValue) bool { return true }
func (v *MockKVTransactionValidator) Txn(kv KeyValue) (KVTransaction, error) {
	return &MockKVTransaction{
		updated: false,
		old:     kv.Value,
		new:     kv.Value,
	}, nil
}

type MockCoordinatingEngine struct {
	MockEngineInstance
	MockTxnCoordinator

	hook struct {
		start    func(*Transaction) (bool, error)
		beginkv  func(*Transaction, *Node, string) (*KeyValue, error)
		commit   func(*Transaction, []*TransactionOperation) (bool, error)
		rollback func(*Transaction) error
	}
}

func (m *MockCoordinatingEngine) TransactionStart(txn *Transaction) (bool, error) {
	s := m.hook.start
	if s == nil {
		return m.MockTxnCoordinator.TransactionStart(txn)
	}
	return s(txn)
}

func (m *MockCoordinatingEngine) TransactionBeginKV(txn *Transaction, node *Node, key string) (*KeyValue, error) {
	s := m.hook.beginkv
	if s == nil {
		return m.MockTxnCoordinator.TransactionBeginKV(txn, node, key)
	}
	return s(txn, node, key)
}

func (m *MockCoordinatingEngine) TransactionCommit(txn *Transaction, rs []*TransactionOperation) (bool, error) {
	s := m.hook.commit
	if s == nil {
		return m.MockTxnCoordinator.TransactionCommit(txn, rs)
	}
	return s(txn, rs)
}

func (m *MockCoordinatingEngine) TransactionRollback(txn *Transaction) error {
	s := m.hook.rollback
	if s == nil {
		return m.MockTxnCoordinator.TransactionRollback(txn)
	}
	return s(txn)
}

func TestOperation(t *testing.T) {
	t.Run("test_select", func(t *testing.T) {
		c, self, err := newTestFakedCluster(nil, nil, nil)
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

		removed, err := c.RemoveNode(n1)
		assert.NoError(t, err)
		assert.True(t, removed)
	})

}
