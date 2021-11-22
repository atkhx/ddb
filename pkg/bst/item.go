// Package bst implements Binary Search Tree algorithm
//
// RU BST https://ru.wikipedia.org/wiki/Двоичное_дерево_поиска
// https://www.cs.usfca.edu/~galles/visualization/BST.html
package bst

import (
	"github.com/atkhx/ddb/pkg/key"
)

type Item struct {
	left  *Item
	right *Item
	key   key.Key
	data  interface{}
}

func New() *Item {
	return &Item{}
}

func (p *Item) IsEmpty() bool {
	return p.key == nil
}

func (p *Item) Search(k key.Key) *Item {
	for !p.IsEmpty() {
		switch p.key.CompareWith(k) {
		case key.CompareResultEqual:
			return p
		case key.CompareResultLess:
			p = p.right
		case key.CompareResultGreater:
			p = p.left
		}
	}
	return nil
}

func (p *Item) Insert(k key.Key) *Item {
	for {
		if p.IsEmpty() {
			*p = Item{
				left:  &Item{},
				right: &Item{},
				key:   k,
				data:  nil,
			}
			return p
		}

		switch p.key.CompareWith(k) {
		case key.CompareResultEqual:
			return p
		case key.CompareResultLess:
			p = p.right
		case key.CompareResultGreater:
			p = p.left
		}
	}
}

func (p *Item) Delete(k key.Key) bool {
	for !p.IsEmpty() {
		switch p.key.CompareWith(k) {
		case key.CompareResultLess:
			p = p.right
			continue
		case key.CompareResultGreater:
			p = p.left
			continue
		}

		// case 1. no child
		if p.left.IsEmpty() && p.right.IsEmpty() {
			p.key = nil
			return true
		}

		// case 2. no left - replace self by right
		if p.left.IsEmpty() {
			*p = *p.right
			return true
		}

		// case 3. no right - replace self by left
		if p.right.IsEmpty() {
			*p = *p.left
			return true
		}

		// case 4. no right.left - replace self by right, safe left in right.left
		if p.right.left.IsEmpty() {
			*p.right.left = *p.left
			*p = *p.right
			return true
		}

		// case 5. replace self by min from right
		minInRight := p.right.Min()
		p.key = minInRight.key

		if !minInRight.right.IsEmpty() {
			*minInRight = *minInRight.right
		} else {
			minInRight.key = nil
		}

		return true
	}

	return false
}

func (p *Item) ScanAsc(fn func(*Item) bool) bool {
	if p.IsEmpty() {
		return true
	}

	if !p.left.IsEmpty() {
		if !p.left.ScanAsc(fn) {
			return false
		}
	}

	if !fn(p) {
		return false
	}

	if !p.right.IsEmpty() {
		if !p.right.ScanAsc(fn) {
			return false
		}
	}

	return true
}

func (p *Item) ScanDesc(fn func(*Item) bool) bool {
	if p.IsEmpty() {
		return true
	}

	if !p.right.IsEmpty() {
		if !p.right.ScanDesc(fn) {
			return false
		}
	}

	if !fn(p) {
		return false
	}

	if !p.left.IsEmpty() {
		if !p.left.ScanDesc(fn) {
			return false
		}
	}

	return true
}

func (p *Item) Min() *Item {
	for !p.IsEmpty() {
		if p.left.IsEmpty() {
			return p
		}
		p = p.left
	}
	return nil
}

func (p *Item) Max() *Item {
	for !p.IsEmpty() {
		if p.right.IsEmpty() {
			return p
		}
		p = p.right
	}
	return nil
}
