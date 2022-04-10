package rwtablebptree

import (
	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/bptree"
	"github.com/atkhx/ddb/internal/storage"
	"github.com/pkg/errors"
)

func NewTable(capacity int) *table {
	return &table{tree: bptree.NewTree(capacity)}
}

type table struct {
	tree bptree.Tree
}

func (t *table) Get(key internal.Key) ([]storage.TxRow, error) {
	row := t.tree.Get(key)
	if row == nil {
		return nil, nil
	}

	txItemsRow, ok := row.(*txItems)
	if !ok {
		return nil, errors.New("invalid row type")
	}
	return txItemsRow.getItems(), nil
}

func (t *table) Set(txObj storage.TxObj, key internal.Key, row internal.Row) error {
	r := t.tree.Get(key)
	if r == nil {
		t.tree.Set(key, NewTxItemsWithItem(row, txObj))
		return nil
	}

	txItemsRow, ok := r.(*txItems)
	if !ok {
		return errors.New("invalid row type")
	}

	txItemsRow.addItem(row, txObj)
	return nil
}
