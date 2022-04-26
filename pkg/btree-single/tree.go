package btree_single

import (
	"github.com/atkhx/ddb/internal"
)

func NewTree(capacity int, provider ItemProvider) *tree {
	return &tree{
		provider: provider,
		capacity: capacity,
	}
}

type tree struct {
	//sync.RWMutex
	provider ItemProvider
	capacity int
}

func (t *tree) Get(key internal.Key) []internal.Row {
	//t.RLock()
	//defer t.RUnlock()

	rows := []internal.Row{}
	leaf := t.getLeaf(key)
	for leaf != nil {
		i, ok := t.searchKeyInLeaf(leaf, key)
		if ok {
			rows = append(rows, leaf.rows[i])
		} else {
			return rows
		}

		for j := i + 1; j < len(leaf.keys); i++ {
			if leaf.keys[j] == key {
				rows = append(rows, leaf.rows[j])
			} else {
				return rows
			}
		}

		if leaf.rightID == nil {
			break
		}

		leaf = t.provider.LoadItem(leaf.rightID)
	}

	return rows

	//return t.searchRowInLeaf(t.getLeaf(key), key)
}

func (t *tree) getLeaf(key internal.Key) *item {
	for item := t.provider.GetRootItem(); item != nil; item = t.searchItemInBranch(item, key) {
		if item.isLeaf {
			return item
		}
	}
	return nil
}

func (t *tree) searchKeyInBranch(branch *item, key internal.Key) int {
	for i := 0; i < len(branch.keys); i++ {
		if branch.keys[i].GreaterThan(key) {
			return i
		}

		if branch.keys[i] == key {
			return i + 1
		}
	}
	return len(branch.keys)
}

func (t *tree) searchItemInBranch(branch *item, key internal.Key) *item {
	return t.provider.LoadItem(branch.iids[t.searchKeyInBranch(branch, key)])
}

func (t *tree) searchKeyInLeaf(leaf *item, key internal.Key) (int, bool) {
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

func (t *tree) searchRowInLeaf(leaf *item, key internal.Key) internal.Row {
	i, ok := t.searchKeyInLeaf(leaf, key)
	if ok {
		return leaf.rows[i]
	}
	return nil
}

func (t *tree) Set(key internal.Key, row internal.Row) {
	//t.Lock()
	//defer t.Unlock()

	leaf := t.getLeaf(key)
	t.insertRowInLeaf(leaf, key, row)
	t.provider.SaveItem(leaf)

	curItem := leaf
	splitKey, newItem := t.splitLeaf(leaf)
	for newItem != nil {
		t.provider.SaveItem(newItem)
		if newItem.parentID == nil {
			newRoot := t.provider.GetNewBranch()
			newRoot.keys = []internal.Key{splitKey}
			newRoot.iids = []ItemID{curItem.itemID, newItem.itemID}

			curItem.parentID = newRoot.itemID
			newItem.parentID = newRoot.itemID

			t.provider.SaveItem(newRoot)
			break
		}

		branch := t.provider.LoadItem(newItem.parentID)
		idx := t.searchKeyInBranch(branch, key)

		t.growBranch(branch, idx, splitKey, newItem)

		curItem = branch
		splitKey, newItem = t.splitBranch(branch)
	}
}

func (t *tree) insertRowInLeaf(leaf *item, key internal.Key, row internal.Row) {
	i, ok := t.searchKeyInLeaf(leaf, key)
	if ok {
		leaf.rows[i] = row
		return
	}

	keys := make([]internal.Key, len(leaf.keys)+1)
	rows := make([]internal.Row, len(leaf.rows)+1)

	if i > 0 {
		copy(keys[:i], leaf.keys[:i])
		copy(rows[:i], leaf.rows[:i])
	}

	if i < len(leaf.keys) {
		copy(keys[i+1:], leaf.keys[i:])
		copy(rows[i+1:], leaf.rows[i:])
	}

	keys[i] = key
	rows[i] = row

	leaf.keys = keys
	leaf.rows = rows
}

func (t *tree) splitLeaf(leaf *item) (internal.Key, *item) {
	if len(leaf.keys) >= t.capacity {
		i := t.capacity / 2
		if t.capacity%2 > 0 {
			i++
		}

		newLeaf := t.provider.GetNewLeaf()

		newLeaf.parentID = leaf.parentID
		newLeaf.rightID = leaf.rightID
		leaf.rightID = newLeaf.itemID

		newLeaf.keys = make([]internal.Key, len(leaf.keys)-i)
		newLeaf.rows = make([]internal.Row, len(leaf.rows)-i)

		copy(newLeaf.keys, leaf.keys[i:])
		copy(newLeaf.rows, leaf.rows[i:])

		leaf.keys = leaf.keys[:i]
		leaf.rows = leaf.rows[:i]

		return newLeaf.keys[0], newLeaf
	}
	return nil, nil
}

func (t *tree) splitBranch(branch *item) (internal.Key, *item) {
	if len(branch.keys) >= t.capacity {
		i := t.capacity / 2

		splitKey := branch.keys[i]

		newBranch := t.provider.GetNewBranch()

		newBranch.parentID = branch.parentID
		newBranch.rightID = branch.rightID
		branch.rightID = newBranch.itemID

		newBranch.keys = make([]internal.Key, len(branch.keys)-i-1)
		newBranch.iids = make([]ItemID, len(branch.iids)-i-1)

		copy(newBranch.keys, branch.keys[i+1:])
		copy(newBranch.iids, branch.iids[i+1:])

		branch.keys = branch.keys[:i]
		branch.iids = branch.iids[:i+1]

		return splitKey, newBranch
	}
	return nil, nil
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
