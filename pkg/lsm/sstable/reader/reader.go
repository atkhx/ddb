package reader

import (
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/pkg/errors"
)

type LenByteReader interface {
	Read() ([]byte, error)
}

type UnSerializer interface {
	RowFromBytes([]byte) (storage.Row, error)
}

func NewReader(origin LenByteReader, unSerializer UnSerializer) *reader {
	return &reader{origin: origin, unSerializer: unSerializer}
}

type reader struct {
	origin       LenByteReader
	unSerializer UnSerializer
}

func (rdr *reader) ReadRow() (int64, storage.Row, error) {
	b, err := rdr.origin.Read()
	if err != nil {
		return 0, nil, errors.Wrap(err, "read row failed")
	}

	r, err := rdr.unSerializer.RowFromBytes(b)
	if err != nil {
		return 0, nil, errors.Wrap(err, "create row failed")
	}

	return int64(4 + len(b)), r, nil
}
