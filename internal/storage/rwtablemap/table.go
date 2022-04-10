package rwtablemap

import (
	"sort"
	"sync"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/storage"
)

func NewTable() *table {
	return &table{data: map[internal.Key][]storage.TxRow{}}
}

type item struct {
	txRow internal.Row
	txObj storage.TxObj
}

func (i *item) GetTxObj() storage.TxObj {
	return i.txObj
}

func (i *item) GetTxRow() internal.Row {
	return i.txRow
}

type table struct {
	sync.RWMutex
	data map[internal.Key][]storage.TxRow
}

func (t *table) Get(key internal.Key) ([]storage.TxRow, error) {
	t.RLock()
	defer t.RUnlock()
	if items, ok := t.data[key]; ok {
		sort.Slice(items, func(i, j int) bool {
			return items[i].GetTxObj().GetTime().After(items[j].GetTxObj().GetTime())
		})
		return items, nil
	}
	return nil, nil
}

func (t *table) Set(txObj storage.TxObj, key internal.Key, row internal.Row) error {
	t.Lock()
	defer t.Unlock()

	t.data[key] = append(t.data[key], &item{
		txRow: row,
		txObj: txObj,
	})
	return nil
}
