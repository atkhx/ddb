package writer

import (
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/pkg/errors"
)

type LenByteWriter interface {
	Write(data []byte) error
	Flush() error
}

func NewWriter(origin LenByteWriter) *writer {
	return &writer{origin: origin}
}

type writer struct {
	origin LenByteWriter
}

func (w *writer) Write(row storage.Row) error {
	b, err := row.Serialize()
	if err != nil {
		return errors.Wrap(err, "serialize row failed")
	}

	if err := w.origin.Write(b); err != nil {
		return errors.Wrap(err, "write row failed")
	}
	return nil
}
