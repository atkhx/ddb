package storage

import (
	"sync"

	"github.com/atkhx/ddb/internal"
)

func NewSSTables() *ssTables {
	return &ssTables{}
}

type ssTables struct {
	sync.RWMutex
	tables []ROTable
}

func (tt *ssTables) Grow(table ROTable) {
	tt.Lock()
	defer tt.Unlock()

	tt.tables = append(tt.tables, table)
}

func (tt *ssTables) Iterate(fn func(ROTable) bool) {
	// iterate by copy
	tt.RLock()
	defer tt.RUnlock()
	var tables = tt.tables
	for i := len(tables); i > 0; i-- {
		if table := tables[i-1]; fn(table) {
			return
		}
	}
	return
}

func (tt *ssTables) Get(key internal.Key) (row internal.Row, err error) {
	tt.Iterate(func(table ROTable) bool {
		row, err = table.Get(key)
		return row != nil || err != nil
	})
	return
}
