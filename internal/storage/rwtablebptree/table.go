package rwtablebptree

import (
	"sort"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/pkg/btree"
)

func NewTable(capacity int, provider btree.ItemProvider) *table {
	return &table{tree: btree.NewTree(capacity, provider)}
}

type table struct {
	tree btree.Tree
}

func (t *table) Get(key internal.Key) ([]storage.TxRow, error) {
	rows := t.tree.Get(key)
	if rows == nil {
		return nil, nil
	}

	var txRows []storage.TxRow
	for _, row := range rows {
		txRows = append(txRows, row.(storage.TxRow))
	}
	sort.Slice(txRows, func(i, j int) bool {
		return txRows[i].GetTxObj().GetTime().After(txRows[j].GetTxObj().GetTime())
	})

	return txRows, nil
	//txItemsRow, ok := rows.(*txItems)
	//if !ok {
	//	return nil, errors.New("invalid row type")
	//}
	//return txItemsRow.getItems(), nil
}

func (t *table) Set(txObj storage.TxObj, key internal.Key, row internal.Row) error {
	t.tree.Set(key, &txItem{
		txRow: row,
		txObj: txObj,
	})
	return nil
	//r := t.tree.Get(key)
	//if r == nil {
	//	t.tree.Set(key, NewTxItemsWithItem(row, txObj))
	//	return nil
	//}
	//
	//txItemsRow, ok := r.(*txItems)
	//if !ok {
	//	return errors.New("invalid row type")
	//}
	//
	//txItemsRow.addItem(row, txObj)
	//return nil
}
