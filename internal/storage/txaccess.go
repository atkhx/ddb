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
