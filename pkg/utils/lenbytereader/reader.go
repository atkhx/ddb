//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=reader_mocks.go
package lenbytereader

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Reader interface {
	io.Reader
}

func NewReader(originReader Reader) *reader {
	return &reader{origin: originReader}
}

type reader struct {
	origin Reader
}

func (r *reader) Read() ([]byte, error) {
	var size uint32
	if err := binary.Read(r.origin, binary.BigEndian, &size); err != nil {
		return nil, errors.Wrap(err, "read len failed")
	}

	data := make([]byte, int(size))
	if err := binary.Read(r.origin, binary.BigEndian, &data); err != nil {
		return nil, errors.Wrap(err, "read bytes failed")
	}
	return data, nil
}
