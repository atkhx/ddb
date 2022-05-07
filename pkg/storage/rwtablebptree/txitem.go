package rwtablebptree

import (
	"github.com/atkhx/ddb/pkg/storage"
)

type txItem struct {
	txRow interface{}
	txObj storage.TxObj
}

func (i *txItem) GetTxObj() storage.TxObj {
	return i.txObj
}

func (i *txItem) GetTxRow() interface{} {
	return i.txRow
}
