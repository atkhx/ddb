package rwtablebptree

import (
	"sort"
	"sync"

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

func NewTxItemsWithItem(row internal.Row, txObj storage.TxObj) *txItems {
	return &txItems{
		items: []storage.TxRow{
			&txItem{
				txRow: row,
				txObj: txObj,
			},
		},
	}
}

type txItems struct {
	sync.RWMutex
	items []storage.TxRow
}

func (ti *txItems) getItems() []storage.TxRow {
	ti.RLock()
	defer ti.RUnlock()

	res := make([]storage.TxRow, len(ti.items))
	copy(res, ti.items)
	return res
}

func (ti *txItems) addItem(row internal.Row, txObj storage.TxObj) {
	ti.Lock()
	defer ti.Unlock()

	ti.items = append(ti.items, &txItem{
		txRow: row,
		txObj: txObj,
	})

	sort.Slice(ti.items, func(i, j int) bool {
		return ti.items[i].GetTxObj().GetTime().After(ti.items[j].GetTxObj().GetTime())
	})
}
