package rwtablebptree

import "github.com/atkhx/ddb/internal/storage"

func NewFactory() *factory {
	return &factory{}
}

type factory struct {
}

func (f *factory) Create(capacity int) storage.RWTable {
	return NewTable(capacity)
}
