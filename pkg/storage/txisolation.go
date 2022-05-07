package storage

var (
	readCommitted  = NewReadCommitted()
	repeatableRead = NewRepeatableRead()
)

func NewRepeatableRead() *txRepeatableRead {
	return &txRepeatableRead{}
}

type txRepeatableRead struct{}

func (a *txRepeatableRead) SkipLocked() bool {
	return true
}

func (a *txRepeatableRead) IsReadable(rowTx, curTx TxObj) bool {
	if rowTx.GetID() > curTx.GetID() {
		return false
	}

	if rowTx.GetID() == curTx.GetID() {
		return rowTx.GetState() != TxRolledBack
	}

	return rowTx.GetState() == TxCommitted && rowTx.GetTime().Before(curTx.GetTime())
}

func NewReadCommitted() *txAccessReadCommitted {
	return &txAccessReadCommitted{}
}

type txAccessReadCommitted struct{}

func (a *txAccessReadCommitted) SkipLocked() bool {
	return false
}

func (a *txAccessReadCommitted) IsReadable(rowTx, curTx TxObj) bool {
	if rowTx.GetID() == curTx.GetID() {
		return rowTx.GetState() != TxRolledBack
	}

	return rowTx.GetState() == TxCommitted
}
