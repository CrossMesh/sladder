package gossip

import (
	"github.com/sunmxt/sladder"
)

// TransactionCommit is called before commit of a transaction.
func (e *EngineInstance) TransactionCommit(t *sladder.Transaction, ops []*sladder.TransactionOperation) (accepted bool, err error) {
	tid := t.ID()
	_, isEngineTxn := e.innerTxnIDs.Load(tid)
	if isEngineTxn {
		e.innerTxnIDs.Delete(tid)
	}

	if accepted, err = e.enforceTransactionCommitLimit(t, isEngineTxn, ops); !accepted || err != nil {
		return
	}

	if accepted, err = e.ensureTransactionCommitIntegrity(t, isEngineTxn, ops); !accepted || err != nil {
		return
	}

	return true, nil
}
