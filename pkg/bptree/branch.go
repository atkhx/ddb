package bptree

import (
	"sync"

	"github.com/atkhx/ddb/internal"
)

func NewBranch(capacity int) *branch {
	return &branch{cap: capacity}
}

type branch struct {
	sync.RWMutex
	keys  []internal.Key
	items []Item
	cap   int
}

func (b *branch) GetKey(i int) internal.Key {
	if i < len(b.keys) {
		return b.keys[i]
	}
	return nil
}

func (b *branch) KeysCount() int {
	return len(b.keys)
}

func (b *branch) findRow(key internal.Key) int {
	for i := 0; i < len(b.keys); i++ {
		if b.keys[i].GreaterThan(key) {
			return i
		}

		if b.keys[i] == key {
			return i + 1
		}
	}
	return len(b.keys)
}

func (b *branch) Get(key internal.Key) internal.Row {
	b.RLock()
	defer b.RUnlock()

	return b.items[b.findRow(key)].Get(key)
}

func (b *branch) Set(key internal.Key, row internal.Row) {
	b.Lock()
	defer b.Unlock()

	idx := b.findRow(key)
	item := b.items[idx]
	item.Set(key, row)

	k, i := item.Split()
	if i != nil {
		keys := make([]internal.Key, len(b.keys)+1)
		items := make([]Item, len(b.items)+1)

		copy(keys[:idx], b.keys[:idx])
		copy(items[:idx+1], b.items[:idx+1])

		copy(keys[idx+1:], b.keys[idx:])
		copy(items[idx+1:], b.items[idx:])

		keys[idx] = k
		items[idx+1] = i

		b.keys = keys
		b.items = items
	}
}

func (b *branch) Split() (internal.Key, Item) {
	if len(b.keys) >= b.cap {
		i := b.cap / 2

		splitKey := b.keys[i]

		newBranch := NewBranch(b.cap)
		newBranch.keys = make([]internal.Key, len(b.keys)-i-1)
		newBranch.items = make([]Item, len(b.items)-i-1)

		copy(newBranch.keys, b.keys[i+1:])
		copy(newBranch.items, b.items[i+1:])

		b.keys = b.keys[:i]
		b.items = b.items[:i+1]

		return splitKey, newBranch
	}
	return nil, nil
}
