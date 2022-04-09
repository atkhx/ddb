package storage

var (
	readCommitted  = NewReadCommitted()
	repeatableRead = NewRepeatableRead()
)

func NewRepeatableRead() *txRepeatableRead {
	return &txRepeatableRead{}
}

type txRepeatableRead struct{}

func (a *txRepeatableRead) IsReadable(originTx, txObj TxObj) bool {
	if originTx.GetID() > txObj.GetID() {
		return false
	}

	if originTx.GetID() == txObj.GetID() {
		return originTx.GetState() != TxRolledBack
	}

	return originTx.GetState() == TxCommitted && originTx.GetTime().Before(txObj.GetTime())
}

func NewReadCommitted() *txAccessReadCommitted {
	return &txAccessReadCommitted{}
}

type txAccessReadCommitted struct{}

func (a *txAccessReadCommitted) IsReadable(originTx, txObj TxObj) bool {
	if originTx.GetID() == txObj.GetID() {
		return originTx.GetState() != TxRolledBack
	}

	return originTx.GetState() == TxCommitted
}
