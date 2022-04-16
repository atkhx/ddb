//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package storage

import (
	"time"

	"github.com/atkhx/ddb/internal"
)

type Storage interface {
	Get(internal.Key) (internal.Row, error)
	Set(key internal.Key, row internal.Row) error
	TxGet(TxObj, internal.Key) (internal.Row, error)
	TxSet(TxObj, internal.Key, internal.Row) error

	Begin(options ...TxOpt) TxObj
	Commit(TxObj) error
	Rollback(TxObj) error

	TxGetForUpdate(TxObj, internal.Key) (internal.Row, error)
	LockKeys(txObj TxObj, keys []internal.Key) error
}

type TxManager interface {
	Begin(options ...TxOpt) TxObj

	Commit(TxObj) error
	Rollback(TxObj) error

	Get(TxObj, internal.Key) (internal.Row, error)
	Set(TxObj, internal.Key, internal.Row) error
}

type TxFactory interface {
	Create(...TxOpt) TxObj
}

type RWTabFactory interface {
	Create() RWTable
}

type TxIsolation interface {
	SkipLocked() bool
	IsReadable(originTx, txObj TxObj) bool
}

type TxRow interface {
	GetTxRow() internal.Row
	GetTxObj() TxObj
}

type RWTable interface {
	Get(internal.Key) ([]TxRow, error)
	Set(TxObj, internal.Key, internal.Row) error
}

type ROTable interface {
	Get(internal.Key) (internal.Row, error)
}

type ROTables interface {
	Grow(ROTable)
	Get(internal.Key) (internal.Row, error)
}

type Locks interface {
	LockKey(txID int64, skipLocked bool, key internal.Key) error
	LockKeys(txID int64, skipLocked bool, keys ...internal.Key) error
	Release(txID int64)
}

type TxObj interface {
	GetID() int64
	GetState() TxState
	GetTime() time.Time

	IsWriteable() bool
	IsReadable() bool

	GetIsolation() TxIsolation

	commit()
	rollback()
}

type TxLockWaitFactory interface {
	Create() waitChan
}
