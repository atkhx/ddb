package storage

import (
	"github.com/atkhx/ddb/pkg/key"
	"github.com/atkhx/ddb/pkg/walog"
)

type Row interface {
	Key() key.Key
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
	Search(key.Key) (Row, error)
	Insert(Row) error
	Reset()
}

type SSTable interface {
	Search(k key.Key) (Row, error)
	Close() error
}

type MemFlusher interface {
	Flush(MemTable) (SSTable, error)
}
