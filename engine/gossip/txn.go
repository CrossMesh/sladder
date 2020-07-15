package gossip

import (
	"github.com/sunmxt/sladder"
)

// TransactionCommit is called before commit of a transaction.
func (e *EngineInstance) TransactionCommit(t *sladder.Transaction, rcs []*sladder.KVTransactionRecord) (accepted bool, err error) {
	if accepted, err = e.enforceTransactionCommitLimit(t, rcs); !accepted || err != nil {
		return
	}

	if accepted, err = e.tagTraceTransactionCommit(t, rcs); !accepted || err != nil {
		return
	}

	return true, nil
}
