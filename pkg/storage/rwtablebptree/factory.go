package rwtablebptree

import (
	"github.com/atkhx/ddb/pkg/bptree"
	"github.com/atkhx/ddb/pkg/storage"
)

func NewFactory() *factory {
	return &factory{}
}

type factory struct {
}

func (f *factory) Create(capacity int, provider bptree.ItemProvider) storage.RWTable {
	return NewTable(capacity, provider)
}
