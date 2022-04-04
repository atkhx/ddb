package rwtablemap

import "github.com/atkhx/ddb/internal/storage"

func NewTable() *table {
	return &table{data: map[storage.Key]storage.Row{}}
}

type table struct {
	data map[storage.Key]storage.Row
}

func (t *table) Get(key storage.Key) (storage.Row, error) {
	return t.data[key], nil
}

func (t *table) Set(key storage.Key, row storage.Row) error {
	t.data[key] = row
	return nil
}
