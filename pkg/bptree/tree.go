package bptree

import (
	"sync"

	"github.com/atkhx/ddb/pkg/base"
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

func (t *tree) getLeftLeaf() (*item, error) {
	leaf, err := t.provider.GetRootItem()
	if err != nil {
		return nil, err
	}

	for leaf != nil && !leaf.isLeaf {
		leaf, err = t.provider.LoadItem(leaf.iids[0])
		if err != nil {
			return nil, err
		}
	}

	return leaf, nil
}

func (t *tree) getRightLeaf() (*item, error) {
	leaf, err := t.provider.GetRootItem()
	if err != nil {
		return nil, err
	}

	for leaf != nil && !leaf.isLeaf {
		leaf, err = t.provider.LoadItem(leaf.iids[len(leaf.iids)-1])
		if err != nil {
			return nil, err
		}
	}
	return leaf, nil
}

func (t *tree) ScanASC(fn func(row interface{}) bool) error {
	t.RLock()
	defer t.RUnlock()

	leaf, err := t.getLeftLeaf()
	if err != nil {
		return err
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

func (t *tree) ScanDESC(fn func(row interface{}) bool) error {
	t.RLock()
	defer t.RUnlock()

	leaf, err := t.getRightLeaf()
	if err != nil {
		return err
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

func (t *tree) Get(key base.Key) (rows []interface{}, err error) {
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

func (t *tree) getLeafASC(key base.Key) (*item, error) {
	item, err := t.provider.GetRootItem()
	if err != nil {
		return nil, err
	}

	for item != nil && !item.isLeaf {
		item, err = t.provider.LoadItem(item.iids[t.searchKeyInBranchASC(item, key)])
		if err != nil {
			return nil, err
		}
	}
	return item, nil
}

func (t *tree) searchKeyInBranchASC(branch *item, key base.Key) int {
	for i := 0; i < len(branch.keys); i++ {
		if branch.keys[i].CompareWith(key).IsLess() {
			continue
		}
		return i
	}
	return len(branch.keys)
}

func (t *tree) searchKeyInLeafASC(leaf *item, key base.Key) (int, bool) {
	for i := 0; i < len(leaf.keys); i++ {
		if cmp := leaf.keys[i].CompareWith(key); cmp.IsLess() {
			continue
		} else {
			return i, cmp.IsEqual()
		}
	}
	return len(leaf.keys), false
}

func (t *tree) getPathForAdd(key base.Key) (searchPath searchPath, err error) {
	item, err := t.provider.GetRootItem()
	if err != nil {
		return nil, err
	}

	for item != nil {
		var idx int
		for idx = 0; idx < len(item.keys); idx++ {
			if item.keys[idx].CompareWith(key).IsGreater() {
				break
			}
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

func (t *tree) Add(key base.Key, row interface{}) error {
	t.Lock()
	defer t.Unlock()

	var splitKey base.Key
	var newItem *item

	searchPath, err := t.getPathForAdd(key)
	if err != nil {
		return err
	}

	for {
		item := searchPath[len(searchPath)-1].item
		kidx := searchPath[len(searchPath)-1].kidx

		searchPath = searchPath[:len(searchPath)-1]
		itemIsRoot := item.isRoot

		if item.isLeaf {
			t.insertRowInLeaf(item, kidx, key, row)
		} else {
			t.growBranch(item, kidx, splitKey, newItem)
		}

		splitKey, newItem, err = t.splitItem(item)
		if err != nil {
			return err
		}

		if err = t.provider.SaveItem(item); err != nil {
			return err
		}

		if newItem == nil {
			break
		}

		if err = t.provider.SaveItem(newItem); err != nil {
			return err
		}

		if itemIsRoot {
			if newRoot, err := t.growRoot(splitKey, item, newItem); err != nil {
				return err
			} else {
				return t.provider.SaveItem(newRoot)
			}
		}
	}
	return nil
}

func (t *tree) insertRowInLeaf(leaf *item, idx int, nkey base.Key, nrow interface{}) {
	keys := make([]base.Key, len(leaf.keys)+1)
	rows := make([]interface{}, len(leaf.rows)+1)

	if idx > 0 {
		copy(keys[:idx], leaf.keys[:idx])
		copy(rows[:idx], leaf.rows[:idx])
	}

	if idx < len(leaf.keys) {
		copy(keys[idx+1:], leaf.keys[idx:])
		copy(rows[idx+1:], leaf.rows[idx:])
	}

	keys[idx] = nkey
	rows[idx] = nrow

	leaf.keys = keys
	leaf.rows = rows
}

func (t *tree) splitItem(item *item) (base.Key, *item, error) {
	if item.isLeaf {
		return t.splitLeaf(item)
	}
	return t.splitBranch(item)
}

func (t *tree) splitLeaf(leaf *item) (base.Key, *item, error) {
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
		newLeaf.isRoot = false

		leaf.rightID = newLeaf.itemID
		leaf.isRoot = false

		newLeaf.keys = make([]base.Key, len(leaf.keys)-i)
		newLeaf.rows = make([]interface{}, len(leaf.rows)-i)

		copy(newLeaf.keys, leaf.keys[i:])
		copy(newLeaf.rows, leaf.rows[i:])

		leaf.keys = leaf.keys[:i]
		leaf.rows = leaf.rows[:i]

		return newLeaf.keys[0], newLeaf, nil
	}
	return nil, nil, nil
}

func (t *tree) splitBranch(branch *item) (base.Key, *item, error) {
	if len(branch.keys) >= t.capacity {
		i := t.capacity / 2

		splitKey := branch.keys[i]

		newBranch, err := t.provider.GetNewBranch()
		if err != nil {
			return nil, nil, err
		}

		newBranch.rightID = branch.rightID
		newBranch.leftID = branch.itemID
		newBranch.isRoot = false

		branch.rightID = newBranch.itemID
		branch.isRoot = false

		newBranch.keys = make([]base.Key, len(branch.keys)-i-1)
		newBranch.iids = make([]ItemID, len(branch.iids)-i-1)

		copy(newBranch.keys, branch.keys[i+1:])
		copy(newBranch.iids, branch.iids[i+1:])

		branch.keys = branch.keys[:i]
		branch.iids = branch.iids[:i+1]

		return splitKey, newBranch, nil
	}
	return nil, nil, nil
}

func (t *tree) growBranch(branch *item, idx int, splitKey base.Key, newItem *item) {
	keys := make([]base.Key, len(branch.keys)+1)
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

func (t *tree) growRoot(splitKey base.Key, curItem, newItem *item) (*item, error) {
	newRoot, err := t.provider.GetNewBranch()
	if err != nil {
		return nil, err
	}

	newRoot.isRoot = true
	newRoot.keys = []base.Key{splitKey}
	newRoot.iids = []ItemID{curItem.itemID, newItem.itemID}

	return newRoot, nil
}
