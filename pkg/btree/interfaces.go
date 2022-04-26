package btree

import (
	"github.com/atkhx/ddb/internal"
)

type ItemProvider interface {
	GetRootItem() *item

	LoadItem(ItemID) *item
	SaveItem(*item)

	GetNewBranch() *item
	GetNewLeaf() *item
}

type Tree interface {
	Get(internal.Key) []internal.Row
	Set(internal.Key, internal.Row)
}

type ItemID interface{}
