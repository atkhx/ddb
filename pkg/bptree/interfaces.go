//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package bptree

import (
	"github.com/atkhx/ddb/internal"
)

type Tree interface {
	Get(internal.Key) internal.Row
	Set(internal.Key, internal.Row)
}

type Item interface {
	Get(internal.Key) internal.Row
	Set(internal.Key, internal.Row)

	Split() (internal.Key, Item)
}
