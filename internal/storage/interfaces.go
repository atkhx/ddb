//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package storage

import (
	"time"

	"github.com/atkhx/ddb/pkg/base"
)

type Storage interface {
	Get(key base.Key) (interface{}, error)
	Set(key base.Key, row interface{}) error
	TxGet(TxObj, base.Key) (interface{}, error)
	TxSet(TxObj, base.Key, interface{}) error

	Begin(options ...TxOpt) TxObj
	Commit(TxObj) error
	Rollback(TxObj) error

	TxGetForUpdate(TxObj, base.Key) (interface{}, error)
	LockKeys(txObj TxObj, keys []base.Key) error
}

type TxManager interface {
	Begin(options ...TxOpt) TxObj

	Commit(TxObj) error
	Rollback(TxObj) error

	Get(TxObj, base.Key) (interface{}, error)
	Set(TxObj, base.Key, interface{}) error
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
	GetTxRow() interface{}
	GetTxObj() TxObj
}

type RWTable interface {
	Get(base.Key) ([]TxRow, error)
	Set(TxObj, base.Key, interface{}) error
}

type ROTable interface {
	Get(base.Key) (interface{}, error)
}

type ROTables interface {
	Grow(ROTable)
	Get(base.Key) (interface{}, error)
}

type Locks interface {
	LockKey(txID int64, skipLocked bool, key base.Key) error
	LockKeys(txID int64, skipLocked bool, keys ...base.Key) error
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
