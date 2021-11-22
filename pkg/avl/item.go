// Package avl implements BST with AVL self-balance algorithm
//
// RU https://ru.wikipedia.org/wiki/АВЛ-дерево
// https://www.cs.usfca.edu/~galles/visualization/AVLtree.html
package avl

import (
	"github.com/atkhx/ddb/pkg/key"
)

type Item struct {
	left   *Item
	right  *Item
	key    key.Key
	height int
	data   interface{}
}

func New() *Item {
	return &Item{height: -1}
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func (p *Item) Balance() (res int) {
	if p.IsEmpty() {
		return 0
	}

	return p.left.height - p.right.height
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
	if p.IsEmpty() {
		*p = Item{
			left:   New(),
			right:  New(),
			height: 0,
			key:    k,
			data:   nil,
		}
		return p
	}

	var res *Item
	switch p.key.CompareWith(k) {
	case key.CompareResultEqual:
		res = p
	case key.CompareResultGreater:
		res = p.left.Insert(k)
		p.ReBalance()
	case key.CompareResultLess:
		res = p.right.Insert(k)
		p.ReBalance()
	}

	return res
}

func (p *Item) ReBalance() {
	p.height = max(p.left.height, p.right.height) + 1

	b := p.Balance()

	if b == -2 {
		if p.right.left.height > p.right.right.height {
			p.BigLeftRotate()
		} else {
			p.MinLeftRotate()
		}
	}

	if b == 2 {
		if p.left.right.height > p.left.left.height {
			p.BigRightRotate()
		} else {
			p.MinRightRotate()
		}
	}
}

func (p *Item) Delete(k key.Key) bool {
	if p.IsEmpty() {
		return false
	}

	switch p.key.CompareWith(k) {
	case key.CompareResultLess:
		r := p.right.Delete(k)
		p.ReBalance()

		return r
	case key.CompareResultGreater:
		r := p.left.Delete(k)
		p.ReBalance()

		return r
	}

	// case 1. no child
	if p.left.IsEmpty() && p.right.IsEmpty() {
		p.key = nil
		p.height = -1
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
		p.ReBalance()
		return true
	}

	// case 5. replace self by min from right
	minInRight := p
	parents := []*Item{}
	for !minInRight.left.IsEmpty() {
		parents = append(parents, minInRight)
		minInRight = minInRight.left
	}

	p.key = minInRight.key

	if !minInRight.right.IsEmpty() {
		*minInRight = *minInRight.right
	} else {
		minInRight.key = nil
		minInRight.height = -1
	}

	for len(parents) > 0 {
		p := parents[len(parents)-1]
		parents = parents[:len(parents)-1]
		p.ReBalance()
	}
	return true
}

func (p *Item) BigLeftRotate() bool {
	if p.IsEmpty() {
		return false
	}

	newRoot := *p.right.left

	p.right.left = newRoot.right
	newRoot.right = p.right

	p.right = newRoot.left

	saveLeft := *p // потому что p перетираем
	newRoot.left = &saveLeft

	*p = newRoot

	p.left.height = max(p.left.left.height, p.left.right.height) + 1
	p.right.height = max(p.right.left.height, p.right.right.height) + 1
	p.height = max(p.left.height, p.right.height) + 1

	return false
}

func (p *Item) MinLeftRotate() bool {
	if p.IsEmpty() || p.right.IsEmpty() {
		return false
	}

	newRoot := *p.right

	*p.right = *newRoot.left
	*newRoot.left = *p

	*p = newRoot

	p.left.height = max(p.left.left.height, p.left.right.height) + 1
	p.height = max(p.left.height, p.right.height) + 1

	return true
}

func (p *Item) BigRightRotate() bool {
	if p.IsEmpty() {
		return false
	}

	newRoot := *p.left.right

	p.left.right = newRoot.left
	newRoot.left = p.left

	p.left = newRoot.right
	saveRight := *p // потому что p перетираем
	newRoot.right = &saveRight

	*p = newRoot

	p.left.height = max(p.left.left.height, p.left.right.height) + 1
	p.right.height = max(p.right.left.height, p.right.right.height) + 1
	p.height = max(p.left.height, p.right.height) + 1

	return false
}

func (p *Item) MinRightRotate() bool {
	if p.IsEmpty() || p.left.IsEmpty() {
		return false
	}

	newRoot := *p.left

	*p.left = *newRoot.right
	*newRoot.right = *p

	newRoot.right.height--
	newRoot.right.height--

	*p = newRoot

	p.right.height = max(p.right.left.height, p.right.right.height) + 1
	p.height = max(p.left.height, p.right.height) + 1

	return true
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

func (p *Item) ScanDown(fn func(*Item) bool) bool {
	if p.IsEmpty() {
		return true
	}

	if !fn(p) {
		return false
	}

	if !p.left.IsEmpty() {
		if !p.left.ScanDown(fn) {
			return false
		}
	}

	if !p.right.IsEmpty() {
		if !p.right.ScanDown(fn) {
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

func (p *Item) IsValid() bool {
	if p.IsEmpty() {
		return true
	}

	res := true

	if !p.left.IsEmpty() {
		res = res && p.left.key.Less(p.key) && p.left.IsValid()
	}

	if !p.right.IsEmpty() {
		res = res && p.key.Less(p.right.key) && p.right.IsValid()
	}

	return res
}
