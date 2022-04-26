package bptree

import (
	"sync"

	"github.com/atkhx/ddb/internal"
)

func NewLeaf(capacity int) *leaf {
	return &leaf{cap: capacity}
}

type leaf struct {
	sync.RWMutex
	keys []internal.Key
	rows []internal.Row
	next *leaf

	cap int
}

func (l *leaf) GetKey(i int) internal.Key {
	if i < len(l.keys) {
		return l.keys[i]
	}
	return nil
}

func (l *leaf) KeysCount() int {
	return len(l.keys)
}

func (l *leaf) findKey(key internal.Key) (int, bool) {
	for i := 0; i < len(l.keys); i++ {
		if l.keys[i] == key {
			return i, true
		}

		if l.keys[i].GreaterThan(key) {
			return i, false
		}
	}
	return len(l.keys), false
}

func (l *leaf) Get(key internal.Key) internal.Row {
	l.RLock()
	defer l.RUnlock()

	i, ok := l.findKey(key)
	if ok {
		return l.rows[i]
	}

	return nil
}

func (l *leaf) Set(key internal.Key, row internal.Row) {
	l.Lock()
	defer l.Unlock()

	i, ok := l.findKey(key)
	if ok {
		l.rows[i] = row
		return
	}

	keys := make([]internal.Key, len(l.keys)+1)
	rows := make([]internal.Row, len(l.rows)+1)

	if i > 0 {
		copy(keys[:i], l.keys[:i])
		copy(rows[:i], l.rows[:i])
	}

	if i < len(l.keys) {
		copy(keys[i+1:], l.keys[i:])
		copy(rows[i+1:], l.rows[i:])
	}

	keys[i] = key
	rows[i] = row

	l.keys = keys
	l.rows = rows
}

func (l *leaf) Split() (internal.Key, Item) {
	if len(l.keys) >= l.cap {
		i := l.cap / 2
		if l.cap%2 > 0 {
			i++
		}

		newLeaf := NewLeaf(l.cap)
		newLeaf.keys = make([]internal.Key, len(l.keys)-i)
		newLeaf.rows = make([]internal.Row, len(l.rows)-i)

		copy(newLeaf.keys, l.keys[i:])
		copy(newLeaf.rows, l.rows[i:])

		l.keys = l.keys[:i]
		l.rows = l.rows[:i]

		newLeaf.next = l.next
		l.next = newLeaf

		return newLeaf.keys[0], newLeaf
	}
	return nil, nil
}
