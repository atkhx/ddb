//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package bptree

import (
	"github.com/atkhx/ddb/pkg/base"
)

type ItemProvider interface {
	GetRootItem() (*item, error)

	LoadItem(ItemID) (*item, error)
	SaveItem(*item) error

	GetNewBranch() (*item, error)
	GetNewLeaf() (*item, error)
}

type Tree interface {
	ScanASC(fn func(row interface{}) bool) error
	ScanDESC(fn func(row interface{}) bool) error

	Get(base.Key) ([]interface{}, error)
	Add(base.Key, interface{}) error
}

type ItemID interface{}
