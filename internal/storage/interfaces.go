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

	Begin(options ...txOpt) TxObj
	Commit(TxObj) error
	Rollback(TxObj) error

	TxGetForUpdate(TxObj, Key) (Row, error)
	LockKeys(txObj TxObj, keys []Key) error
}

type TxManager interface {
	Begin(options ...txOpt) TxObj

	Commit(TxObj) error
	Rollback(TxObj) error

	Get(TxObj, Key) (Row, error)
	Set(TxObj, Key, Row) error
}

type TxFactory interface {
	Create(RWTable, ...txOpt) TxObj
}

type RWTabFactory interface {
	Create() RWTable
}

type TxIsolation interface {
	IsReadable(originTx, txObj TxObj) bool
}

type RWTable interface {
	Get(Key) (Row, error)
	Set(Key, Row) error
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
	GetOptSkipLocked() bool

	IsWriteable() bool
	IsReadable() bool

	GetIsolation() TxIsolation

	getTxTable() RWTable
	commit()
	rollback()
	persist()
}
