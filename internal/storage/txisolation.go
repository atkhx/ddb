package storage

var (
	ReadCommitted  = NewReadCommitted()
	RepeatableRead = NewRepeatableRead()
)

func NewRepeatableRead() *txRepeateableRead {
	return &txRepeateableRead{}
}

type txRepeateableRead struct{}

func (a *txRepeateableRead) IsReadable(originTx, txObj TxObj) bool {
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
