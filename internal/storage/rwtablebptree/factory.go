package rwtablebptree

import (
	"github.com/atkhx/ddb/internal/storage"
	"github.com/atkhx/ddb/pkg/bptree"
)

func NewFactory() *factory {
	return &factory{}
}

type factory struct {
}

func (f *factory) Create(capacity int, provider bptree.ItemProvider) storage.RWTable {
	return NewTable(capacity, provider)
}
