package rwtablebptree

import (
	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/storage"
)

type txItem struct {
	txRow internal.Row
	txObj storage.TxObj
}

func (i *txItem) GetTxObj() storage.TxObj {
	return i.txObj
}

func (i *txItem) GetTxRow() internal.Row {
	return i.txRow
}
