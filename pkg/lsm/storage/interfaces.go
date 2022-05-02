package storage

import (
	"github.com/atkhx/ddb/pkg/base"
	"github.com/atkhx/ddb/pkg/walog"
)

type Row interface {
	Key() base.Key
	Data() interface{}

	IsDeleted() bool
	MakeDeleted()
	Serialize() ([]byte, error)
}

type WalogWriter interface {
	Write(walog.Record) error
	Flush() error
}

type MemTable interface {
	Scan(func(Row) (stop bool, err error)) error
	Search(base.Key) (Row, error)
	Insert(Row) error
	Reset()
}

type SSTable interface {
	Search(k base.Key) (Row, error)
	Close() error
}

type MemFlusher interface {
	Flush(MemTable) (SSTable, error)
}
