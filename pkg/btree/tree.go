package btree

import (
	"sync"

	"github.com/atkhx/ddb/internal"
)

func NewTree(capacity int, provider ItemProvider) *tree {
	return &tree{
		provider: provider,
		capacity: capacity,
	}
}

type tree struct {
	sync.RWMutex
	provider ItemProvider
	capacity int
}

func (t *tree) ScanASC(fn func(row internal.Row) bool) error {
	t.RLock()
	defer t.RUnlock()

	leaf, err := t.provider.GetRootItem()
	if err != nil {
		return err
	}

	for !leaf.isLeaf {
		leaf, err = t.provider.LoadItem(leaf.iids[0])
		if err != nil {
			return err
		}
	}

	for leaf != nil {
		for _, row := range leaf.rows {
			if fn(row) {
				break
			}
		}

		if leaf.rightID == nil {
			break
		}

		leaf, err = t.provider.LoadItem(leaf.rightID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *tree) ScanDESC(fn func(row internal.Row) bool) error {
	t.RLock()
	defer t.RUnlock()

	leaf, err := t.provider.GetRootItem()
	if err != nil {
		return err
	}

	for !leaf.isLeaf {
		leaf, err = t.provider.LoadItem(leaf.iids[len(leaf.iids)-1])
		if err != nil {
			return err
		}
	}

	for leaf != nil {
		for i := len(leaf.rows); i > 0; i-- {
			if fn(leaf.rows[i-1]) {
				break
			}
		}

		if leaf.leftID == nil {
			break
		}

		leaf, err = t.provider.LoadItem(leaf.leftID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *tree) Get(key internal.Key) (rows []internal.Row, err error) {
	t.RLock()
	defer t.RUnlock()

	leaf, err := t.getLeafASC(key)
	if err != nil {
		return nil, err
	}

	for firstLeaf := true; leaf != nil; firstLeaf = false {
		i, ok := t.searchKeyInLeafASC(leaf, key)
		if ok {
			rows = append(rows, leaf.rows[i])

			for j := i + 1; j < len(leaf.keys); j++ {
				if leaf.keys[j] != key {
					return
				}

				rows = append(rows, leaf.rows[j])
			}
		} else if !firstLeaf {
			return
		}

		if leaf.rightID == nil {
			return
		}

		leaf, err = t.provider.LoadItem(leaf.rightID)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (t *tree) getLeafASC(key internal.Key) (*item, error) {
	item, err := t.provider.GetRootItem()
	if err != nil {
		return nil, err
	}

	for !item.isLeaf {
		item, err = t.provider.LoadItem(item.iids[t.searchKeyInBranchASC(item, key)])
		if err != nil {
			return nil, err
		}
	}
	return item, nil
}

func (t *tree) searchKeyInBranchASC(branch *item, key internal.Key) int {
	for i := 0; i < len(branch.keys); i++ {
		if branch.keys[i].GreaterThan(key) {
			return i
		}

		if branch.keys[i] == key {
			return i
		}
	}
	return len(branch.keys)
}

func (t *tree) searchKeyInBranchDESC(branch *item, key internal.Key) int {
	for i := 0; i < len(branch.keys); i++ {
		if branch.keys[i].GreaterThan(key) {
			return i
		}
	}
	return len(branch.keys)
}

func (t *tree) searchKeyInLeafASC(leaf *item, key internal.Key) (int, bool) {
	for i := 0; i < len(leaf.keys); i++ {
		if leaf.keys[i] == key {
			return i, true
		}

		if leaf.keys[i].GreaterThan(key) {
			return i, false
		}
	}
	return len(leaf.keys), false
}

func (t *tree) searchKeyInLeafDESC(leaf *item, key internal.Key) int {
	for i := 0; i < len(leaf.keys); i++ {
		if leaf.keys[i].GreaterThan(key) {
			return i
		}
	}
	return len(leaf.keys)
}

func (t *tree) getLeafForInsert(key internal.Key) (searchPath searchPath, err error) {
	var idx int

	item, err := t.provider.GetRootItem()
	if err != nil {
		return nil, err
	}

	for item != nil {
		if item.isLeaf {
			idx = t.searchKeyInLeafDESC(item, key)
		} else {
			idx = t.searchKeyInBranchDESC(item, key)
		}

		searchPath = append(searchPath, searchPathItem{
			item: item,
			kidx: idx,
		})

		if item.isLeaf {
			return
		}

		item, err = t.provider.LoadItem(item.iids[idx])
		if err != nil {
			return nil, err
		}
	}
	return
}

func (t *tree) Set(key internal.Key, row internal.Row) error {
	t.Lock()
	defer t.Unlock()

	searchPath, err := t.getLeafForInsert(key)
	if err != nil {
		return err
	}

	item := searchPath[len(searchPath)-1].item
	kidx := searchPath[len(searchPath)-1].kidx

	searchPath = searchPath[:len(searchPath)-1]

	t.insertRowInLeaf(item, kidx, key, row)

	if err = t.provider.SaveItem(item); err != nil {
		return err
	}

	splitKey, newItem, err := t.splitLeaf(item)
	if err != nil {
		return err
	}

	for newItem != nil {
		if err = t.provider.SaveItem(newItem); err != nil {
			return err
		}

		if item.isRoot {
			return t.growRoot(splitKey, item, newItem)
		}

		item = searchPath[len(searchPath)-1].item
		kidx = searchPath[len(searchPath)-1].kidx

		searchPath = searchPath[:len(searchPath)-1]

		t.growBranch(item, kidx, splitKey, newItem)

		splitKey, newItem, err = t.splitBranch(item)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *tree) insertRowInLeaf(leaf *item, idx int, key internal.Key, row internal.Row) {
	keys := make([]internal.Key, len(leaf.keys)+1)
	rows := make([]internal.Row, len(leaf.rows)+1)

	if idx > 0 {
		copy(keys[:idx], leaf.keys[:idx])
		copy(rows[:idx], leaf.rows[:idx])
	}

	if idx < len(leaf.keys) {
		copy(keys[idx+1:], leaf.keys[idx:])
		copy(rows[idx+1:], leaf.rows[idx:])
	}

	keys[idx] = key
	rows[idx] = row

	leaf.keys = keys
	leaf.rows = rows
}

func (t *tree) splitLeaf(leaf *item) (internal.Key, *item, error) {
	if len(leaf.keys) >= t.capacity {
		i := t.capacity / 2
		if t.capacity%2 > 0 {
			i++
		}

		newLeaf, err := t.provider.GetNewLeaf()
		if err != nil {
			return nil, nil, err
		}

		newLeaf.rightID = leaf.rightID
		newLeaf.leftID = leaf.itemID
		leaf.rightID = newLeaf.itemID

		newLeaf.keys = make([]internal.Key, len(leaf.keys)-i)
		newLeaf.rows = make([]internal.Row, len(leaf.rows)-i)

		copy(newLeaf.keys, leaf.keys[i:])
		copy(newLeaf.rows, leaf.rows[i:])

		leaf.keys = leaf.keys[:i]
		leaf.rows = leaf.rows[:i]

		return newLeaf.keys[0], newLeaf, nil
	}
	return nil, nil, nil
}

func (t *tree) splitBranch(branch *item) (internal.Key, *item, error) {
	if len(branch.keys) >= t.capacity {
		i := t.capacity / 2

		splitKey := branch.keys[i]

		newBranch, err := t.provider.GetNewBranch()
		if err != nil {
			return nil, nil, err
		}

		newBranch.rightID = branch.rightID
		newBranch.leftID = branch.itemID
		branch.rightID = newBranch.itemID

		newBranch.keys = make([]internal.Key, len(branch.keys)-i-1)
		newBranch.iids = make([]ItemID, len(branch.iids)-i-1)

		copy(newBranch.keys, branch.keys[i+1:])
		copy(newBranch.iids, branch.iids[i+1:])

		branch.keys = branch.keys[:i]
		branch.iids = branch.iids[:i+1]

		return splitKey, newBranch, nil
	}
	return nil, nil, nil
}

func (t *tree) growBranch(branch *item, idx int, splitKey internal.Key, newItem *item) {
	keys := make([]internal.Key, len(branch.keys)+1)
	iids := make([]ItemID, len(branch.iids)+1)

	copy(keys[:idx], branch.keys[:idx])
	copy(iids[:idx+1], branch.iids[:idx+1])

	copy(keys[idx+1:], branch.keys[idx:])
	copy(iids[idx+1:], branch.iids[idx:])

	keys[idx] = splitKey
	iids[idx+1] = newItem.itemID

	branch.keys = keys
	branch.iids = iids
}

func (t *tree) growRoot(splitKey internal.Key, curItem, newItem *item) error {
	newRoot, err := t.provider.GetNewBranch()
	if err != nil {
		return err
	}

	newRoot.isRoot = true
	newRoot.keys = []internal.Key{splitKey}
	newRoot.iids = []ItemID{curItem.itemID, newItem.itemID}

	curItem.isRoot = false
	newItem.isRoot = false

	return t.provider.SaveItem(newRoot)
}
