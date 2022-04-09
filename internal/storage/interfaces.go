//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package storage

import "time"

type Key interface {
}

type Row interface {
}

type Storage interface {
	Get(Key) (Row, error)
	Set(key Key, row Row) error
	TxGet(TxObj, Key) (Row, error)
	TxSet(TxObj, Key, Row) error

	Begin(options ...TxOpt) TxObj
	Commit(TxObj) error
	Rollback(TxObj) error

	TxGetForUpdate(TxObj, Key) (Row, error)
	LockKeys(txObj TxObj, keys []Key) error
}

type TxManager interface {
	Begin(options ...TxOpt) TxObj

	Commit(TxObj) error
	Rollback(TxObj) error

	Get(TxObj, Key) (Row, error)
	Set(TxObj, Key, Row) error
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
	GetTxRow() Row
	GetTxObj() TxObj
}

type RWTable interface {
	Get(Key) ([]TxRow, error)
	Set(TxObj, Key, Row) error
}

type ROTable interface {
	Get(Key) (Row, error)
}

type ROTables interface {
	Grow(ROTable)
	Get(Key) (Row, error)
}

type Locks interface {
	InitLock(txID int64, key Key) (waitChan, error)
	InitLocks(txID int64, keys ...Key) ([]waitChan, error)
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
	persist()
}
