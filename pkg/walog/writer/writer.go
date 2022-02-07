//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=writer_mocks.go
package writer

import (
	"github.com/atkhx/ddb/pkg/walog"
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

func (w *writer) Write(record walog.Record) (int, error) {
	data, err := record.Serialize()
	if err != nil {
		return 0, errors.Wrap(err, "serialize wal record failed")
	}

	if err := w.origin.Write(data); err != nil {
		return 0, errors.Wrap(err, "write record data failed")
	}

	if err := w.origin.Flush(); err != nil {
		return 0, errors.Wrap(err, "flush failed")
	}

	return len(data), nil
}
