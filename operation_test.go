package sladder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockKVTransaction struct {
	updated  bool
	old, new string
}

func (m *MockKVTransaction) Set(key string)        { m.updated, m.new = true, key }
func (m *MockKVTransaction) After() (bool, string) { return m.updated, m.new }
func (m *MockKVTransaction) Before() string        { return m.old }

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
		commit   func(*Transaction, []*KVTransactionRecord) (bool, error)
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

func (m *MockCoordinatingEngine) TransactionCommit(txn *Transaction, rs []*KVTransactionRecord) (bool, error) {
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

		assert.True(t, c.RemoveNode(n1))
	})

	t.Run("test_txn", func(t *testing.T) {
		ei := &MockCoordinatingEngine{}
		ei.MockEngineInstance.On("Init", mock.Anything).Return(nil)
		ei.MockEngineInstance.On("Close").Return(nil)

		c, self, err := newTestFakedCluster(nil, ei, nil)
		assert.NotNil(t, c)
		assert.NotNil(t, self)
		assert.NoError(t, err)

		m1, m2 := &MockKVTransactionValidator{}, &MockKVTransactionValidator{}

		assert.NoError(t, c.RegisterKey("key1", m1, false, 0))
		assert.NoError(t, c.RegisterKey("key2", m2, false, 0))
		assert.NoError(t, c.RegisterKey("key4", m2, false, 0))
		assert.NoError(t, c.RegisterKey("key5", m2, false, 0))

		// coordinator start failure.
		rejectStart := func(*Transaction) (bool, error) { return false, nil }
		ei.hook.start = rejectStart
		assert.Equal(t, ErrRejectedByCoordinator, c.Txn(func(tx *Transaction) bool {
			assert.Fail(t, "should not start transaction.")
			return false
		}))
		testError := errors.New("error1")
		failStart := func(*Transaction) (bool, error) { return false, testError }
		ei.hook.start = failStart
		assert.Equal(t, testError, c.Txn(func(tx *Transaction) bool {
			assert.Fail(t, "should not start transaction.")
			return false
		}))
		ei.hook.start = nil

		// normal commit
		ei.MockTxnCoordinator.On("TransactionStart", mock.Anything).Return(true, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key1").Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key2").Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key3").Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key4").Return(nil, testError)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key5").Return(&KeyValue{
			Key:   "key5",
			Value: "mock1",
		}, nil)
		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(rs []*KVTransactionRecord) bool {
			if len(rs) < 1 {
				return false
			}
			for _, r := range rs {
				if r.Key != "key1" && r.Key != "key2" && r.Key != "key5" {
					return false
				}
			}
			return true
		})).Return(true, nil)
		ei.MockTxnCoordinator.On("TransactionRollback", mock.Anything).Return(nil)
		e := self.get("key5")
		assert.Nil(t, e)
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			tx.RangeNode(func(n *Node) bool {
				assert.Equal(t, self, n)
				return true
			}, false, false)
			{
				k1, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k1.(*MockKVTransaction).Set("v1")
			}
			{
				k2, err := tx.KV(self, "key2")
				assert.NoError(t, err)
				k2.(*MockKVTransaction).Set("v2")
			}
			{
				k1, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k1.(*MockKVTransaction).Set("v3")
			}
			{
				k3, err := tx.KV(self, "key3")
				assert.Equal(t, ErrValidatorMissing, err)
				assert.Nil(t, k3)
			}
			{
				_, err := tx.KV(newNode(nil), "key1")
				assert.Equal(t, ErrInvalidNode, err)
			}
			{
				_, err := tx.KV(self, "key4")
				assert.Equal(t, testError, err)
			}
			{
				k5, err := tx.KV(self, "key5")
				assert.NoError(t, err)
				assert.Equal(t, "mock1", k5.(*MockKVTransaction).Before())
			}
			return true
		}))
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			{
				k1, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k1.(*MockKVTransaction).Set("v2")
			}
			return true
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v2", e.Value)
		// normal rollback.
		assert.NoError(t, c.Txn(func(tx *Transaction) bool {
			{
				k1, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k1.(*MockKVTransaction).Set("v8")
			}
			return false
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v2", e.Value)
		// fail commit.
		failCommit := func(*Transaction, []*KVTransactionRecord) (bool, error) { return false, testError }
		ei.hook.commit = failCommit
		assert.Equal(t, testError, c.Txn(func(tx *Transaction) bool {
			{
				k1, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k1.(*MockKVTransaction).Set("v8")
			}
			return true
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v2", e.Value)
		// reject commit
		rejectCommit := func(*Transaction, []*KVTransactionRecord) (bool, error) { return false, nil }
		ei.hook.commit = rejectCommit
		assert.Equal(t, ErrRejectedByValidator, c.Txn(func(tx *Transaction) bool {
			{
				k1, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k1.(*MockKVTransaction).Set("v8")
			}
			return true
		}))
		e = self.get("key1")
		assert.NotNil(t, e)
		assert.Equal(t, "key1", e.Key)
		assert.Equal(t, "v2", e.Value)
		e = self.get("key2")
		assert.NotNil(t, e)
		assert.Equal(t, "key2", e.Key)
		assert.Equal(t, "v2", e.Value)
		ei.hook.commit = nil
		// fail rollback.
		failRollback := func(*Transaction) error { return testError }
		ei.hook.rollback = failRollback
		assert.Equal(t, testError, c.Txn(func(tx *Transaction) bool { return false }))

		assert.NoError(t, c.RegisterKey("key1", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key2", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key4", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key5", nil, true, 0))
	})

	t.Run("test_txn_record_order", func(t *testing.T) {
		ei := &MockCoordinatingEngine{}
		ei.MockEngineInstance.On("Init", mock.Anything).Return(nil)
		ei.MockEngineInstance.On("Close").Return(nil)

		c, self, err := newTestFakedCluster(nil, ei, nil)
		assert.NotNil(t, c)
		assert.NotNil(t, self)
		assert.NoError(t, err)

		m1, m2 := &MockKVTransactionValidator{}, &MockKVTransactionValidator{}

		var lastOrder []*KVTransactionRecord

		assert.NoError(t, c.RegisterKey("key1", m1, false, 0))
		assert.NoError(t, c.RegisterKey("key2", m2, false, 0))
		assert.NoError(t, c.RegisterKey("key3", m2, false, 0))
		assert.NoError(t, c.RegisterKey("key4", m2, false, 0))
		ei.MockTxnCoordinator.On("TransactionStart", mock.Anything).Return(true, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(rs []*KVTransactionRecord) bool {
			if len(rs) < 1 {
				return false
			}
			for _, r := range rs {
				if r.Key != "key1" && r.Key != "key2" && r.Key != "key3" && r.Key != "key4" {
					return false
				}
			}
			lastOrder = rs
			return true
		})).Return(true, nil)
		ei.MockTxnCoordinator.On("TransactionRollback", mock.Anything).Return(nil)

		c.Txn(func(tx *Transaction) bool {
			{
				k, err := tx.KV(self, "key1")
				assert.NoError(t, err)
				k.(*MockKVTransaction).Set("v1")
			}
			{
				k, err := tx.KV(self, "key2")
				assert.NoError(t, err)
				k.(*MockKVTransaction).Set("v1.2")
			}
			{
				k, err := tx.KV(self, "key3")
				assert.NoError(t, err)
				k.(*MockKVTransaction).Set("v1.3")
			}
			{
				k, err := tx.KV(self, "key2")
				assert.NoError(t, err)
				k.(*MockKVTransaction).Set("v2")
			}
			{
				k, err := tx.KV(self, "key4")
				assert.NoError(t, err)
				k.(*MockKVTransaction).Set("v1.4")
			}
			return true
		})
		e1, e2, e3, e4 := self.get("key1"), self.get("key2"), self.get("key3"), self.get("key4")
		assert.NotNil(t, e1)
		assert.NotNil(t, e2)
		assert.NotNil(t, e3)
		assert.NotNil(t, e4)
		assert.Equal(t, "key1", e1.Key)
		assert.Equal(t, "key2", e2.Key)
		assert.Equal(t, "key3", e3.Key)
		assert.Equal(t, "key4", e4.Key)
		assert.Equal(t, "v1", e1.Value)
		assert.Equal(t, "v2", e2.Value)
		assert.Equal(t, "v1.3", e3.Value)
		assert.Equal(t, "v1.4", e4.Value)
		assert.NotNil(t, lastOrder)
		assert.Equal(t, 4, len(lastOrder))
		assert.Equal(t, "key1", lastOrder[0].Key)
		assert.Equal(t, "key3", lastOrder[1].Key)
		assert.Equal(t, "key2", lastOrder[2].Key)
		assert.Equal(t, "key4", lastOrder[3].Key)

		assert.NoError(t, c.RegisterKey("key1", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key2", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key3", nil, true, 0))
		assert.NoError(t, c.RegisterKey("key4", nil, true, 0))
	})
}
