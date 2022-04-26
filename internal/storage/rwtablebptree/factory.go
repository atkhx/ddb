package rwtablebptree

import (
	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/pkg/btree"
)

func NewFactory() *factory {
	return &factory{}
}

type factory struct {
}

func (f *factory) Create(capacity int, provider btree.ItemProvider) storage.RWTable {
	return NewTable(capacity, provider)
}
