//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=interfaces_mocks.go
package storage

import "time"

type Key interface {
}

type Row interface {
}

type TxFactory interface {
	Create() TxObj
}

type RWTabFactory interface {
	Create() RWTable
}

type TxTables interface {
	Begin() int64

	Commit(txID int64) error
	Rollback(txID int64) error

	Get(txID int64, key Key) (Row, error)
	Set(txID int64, key Key, row Row) error
}

type TxAccess interface {
	IsReadable(originTx, txObj TxObj) bool
	IsWriteable(originTx TxObj) bool
}

type RWTable interface {
	Get(Key) (Row, error)
	Set(Key, Row) error
}

type ROTable interface {
	Get(Key) (Row, error)
}

type ROTables interface {
	Get(Key) (Row, error)
}

type Locks interface {
	InitLock(txID int64, key Key) (waitChan, error)
	Release(txID int64)
}

type TxObj interface {
	GetID() int64
	GetState() TxState
	GetTime() time.Time

	commit()
	rollback()
}
