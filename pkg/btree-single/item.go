package btree_single

import (
	"github.com/atkhx/ddb/internal"
)

type item struct {
	isLeaf bool

	itemID   ItemID
	parentID ItemID
	rightID  ItemID

	keys []internal.Key
	rows []internal.Row
	iids []ItemID
}
