package btree

import (
	"github.com/atkhx/ddb/internal"
)

type item struct {
	isLeaf bool
	isRoot bool

	itemID  ItemID
	rightID ItemID
	leftID  ItemID

	keys []internal.Key
	rows []internal.Row
	iids []ItemID
}

type searchPath []searchPathItem

type searchPathItem struct {
	item *item
	kidx int
}
