package bptree

import (
	"sync"

	"github.com/atkhx/ddb/internal"
)

func NewTree(capacity int) *tree {
	return &tree{capacity: capacity}
}

type tree struct {
	sync.RWMutex
	root Item

	capacity int
}

func (t *tree) Get(key internal.Key) internal.Row {
	if t.root != nil {
		return t.root.Get(key)
	}
	return nil
}

func (t *tree) Set(key internal.Key, row internal.Row) {
	if t.root == nil {
		t.root = NewLeaf(t.capacity)
	}
	t.root.Set(key, row)

	t.Lock()
	if k, i := t.root.Split(); i != nil {
		newRoot := NewBranch(t.capacity)
		newRoot.keys = []internal.Key{k}
		newRoot.items = []Item{t.root, i}
		t.root = newRoot
	}
	t.Unlock()
}
