package bptree

import (
	"github.com/atkhx/ddb/pkg/base"
)

type item struct {
	isLeaf bool
	isRoot bool

	itemID  ItemID
	rightID ItemID
	leftID  ItemID

	keys []base.Key
	rows []interface{}
	iids []ItemID
}

type searchPath []searchPathItem

type searchPathItem struct {
	item *item
	kidx int
}
