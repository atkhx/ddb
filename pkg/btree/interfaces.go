//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package btree

import (
	"github.com/atkhx/ddb/internal"
)

type ItemProvider interface {
	GetRootItem() (*item, error)

	LoadItem(ItemID) (*item, error)
	SaveItem(*item) error

	GetNewBranch() (*item, error)
	GetNewLeaf() (*item, error)
}

type Tree interface {
	ScanASC(fn func(row internal.Row) bool) error
	ScanDESC(fn func(row internal.Row) bool) error

	Get(internal.Key) ([]internal.Row, error)
	Set(internal.Key, internal.Row) error
}

type ItemID interface{}
