package storage

func NewTxAccess() *txAccess {
	return &txAccess{}
}

type txAccess struct{}

func (a *txAccess) IsReadable(originTx, txObj TxObj) bool {
	if originTx.GetID() > txObj.GetID() {
		return false
	}

	if originTx.GetID() == txObj.GetID() {
		return originTx.GetState() != TxRolledBack
	}

	return originTx.GetState() == TxCommitted && originTx.GetTime().Before(txObj.GetTime())
}

func (a *txAccess) IsWriteable(originTx TxObj) bool {
	return originTx.GetState() == TxUncommitted
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

func (a *txAccessReadCommitted) IsWriteable(originTx TxObj) bool {
	return originTx.GetState() == TxUncommitted
}
