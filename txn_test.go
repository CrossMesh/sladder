package sladder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestTxn(t *testing.T) {
	t.Run("txn", func(t *testing.T) {
		ei := &MockCoordinatingEngine{}
		ei.MockEngineInstance.On("Init", mock.Anything).Return(nil)
		ei.MockEngineInstance.On("Close").Return(nil)
		ei.MockTxnCoordinator.On("TransactionStart", mock.Anything).Return(true, nil)
		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(rs []*TransactionOperation) bool {
			for _, r := range rs {
				if r.Txn != nil && (r.PastExists != r.Exists || r.Updated) {
					return false
				}
			}
			return true
		})).Return(true, nil)

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
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key1").Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key2").Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key3").Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key4").Return(nil, testError)
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, "key5").Return(&KeyValue{
			Key:   "key5",
			Value: "mock1",
		}, nil)
		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(rs []*TransactionOperation) bool {
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
		failCommit := func(*Transaction, []*TransactionOperation) (bool, error) { return false, testError }
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
		rejectCommit := func(*Transaction, []*TransactionOperation) (bool, error) { return false, nil }
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

	t.Run("txn_record_order", func(t *testing.T) {
		var c *Cluster
		var self *Node
		var err error

		ei := &MockCoordinatingEngine{}
		ei.MockEngineInstance.On("Init", mock.Anything).Return(nil)
		ei.MockEngineInstance.On("Close").Return(nil)
		ei.MockTxnCoordinator.On("TransactionStart", mock.Anything).Return(true, nil)
		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(ops []*TransactionOperation) bool {
			return c == nil || self == nil
		})).Return(true, nil)

		c, self, err = newTestFakedCluster(nil, ei, nil)
		assert.NotNil(t, c)
		assert.NotNil(t, self)
		assert.NoError(t, err)

		m1, m2 := &MockKVTransactionValidator{}, &MockKVTransactionValidator{}

		var lastOrder []*TransactionOperation

		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(ops []*TransactionOperation) bool {
			for _, op := range ops {
				if op.Txn == nil {
					return false
				}
				if op.Updated || op.PastExists != op.Exists {
					return false
				}
			}
			return true
		})).Return(true, nil)

		assert.NoError(t, c.RegisterKey("key1", m1, false, 0))
		assert.NoError(t, c.RegisterKey("key2", m2, false, 0))
		assert.NoError(t, c.RegisterKey("key3", m2, false, 0))
		assert.NoError(t, c.RegisterKey("key4", m2, false, 0))
		ei.MockTxnCoordinator.On("TransactionBeginKV", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		ei.MockTxnCoordinator.On("TransactionCommit", mock.Anything, mock.MatchedBy(func(rs []*TransactionOperation) bool {
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
