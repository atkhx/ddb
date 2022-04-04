package rwtablemap

import "github.com/atkhx/ddb/internal/storage"

func NewFactory() *factory {
	return &factory{}
}

type factory struct {
}

func (f *factory) Create() storage.RWTable {
	return NewTable()
}
