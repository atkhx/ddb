//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=writer_mocks.go
package lenbytewriter

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Writer interface {
	io.Writer
}

func NewWriter(origin Writer) *writer {
	return &writer{origin: origin}
}

type writer struct {
	origin Writer
}

func (w *writer) WriteLengthWithData(data []byte) error {
	if err := binary.Write(w.origin, binary.BigEndian, uint32(len(data))); err != nil {
		return errors.Wrap(err, "write len bytes failed")
	}

	if err := binary.Write(w.origin, binary.BigEndian, data); err != nil {
		return errors.Wrap(err, "write row bytes failed")
	}
	return nil
}
