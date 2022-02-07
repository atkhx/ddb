//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=reader_mocks.go
package reader

import (
	"github.com/atkhx/ddb/pkg/walog"
	"github.com/pkg/errors"
)

type LenByteReader interface {
	Read() ([]byte, error)
}

func NewReader(origin LenByteReader) *reader {
	return &reader{origin: origin}
}

type reader struct {
	origin LenByteReader
}

func (r *reader) Read() (walog.Record, error) {
	b, err := r.origin.Read()
	if err != nil {
		return walog.Record{}, errors.Wrap(err, "read data failed")
	}

	rec, err := walog.UnSerialize(b)
	if err != nil {
		return walog.Record{}, errors.Wrap(err, "unserialize wal record failed")
	}
	return rec, nil
}
