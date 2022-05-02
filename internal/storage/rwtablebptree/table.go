package rwtablebptree

import (
	"sort"

	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/pkg/base"
	"github.com/atkhx/ddb/pkg/bptree"
)

func NewTable(capacity int, provider bptree.ItemProvider) *table {
	return &table{tree: bptree.NewTree(capacity, provider)}
}

type table struct {
	tree bptree.Tree
}

func (t *table) Get(key base.Key) ([]storage.TxRow, error) {
	rows, err := t.tree.Get(key)
	if rows == nil || err != nil {
		return nil, err
	}

	var txRows []storage.TxRow
	for _, row := range rows {
		txRows = append(txRows, row.(storage.TxRow))
	}
	sort.Slice(txRows, func(i, j int) bool {
		return txRows[i].GetTxObj().GetTime().After(txRows[j].GetTxObj().GetTime())
	})

	return txRows, nil
}

func (t *table) Set(txObj storage.TxObj, key base.Key, row interface{}) error {
	return t.tree.Set(key, &txItem{
		txRow: row,
		txObj: txObj,
	})
}
