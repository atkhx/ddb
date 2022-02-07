//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=buffered_writer_mocks.go
package lenbytewriter

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

type LBWriter interface {
	WriteLengthWithData(data []byte) error
}

type Flusher interface {
	Flush() error
}

func NewInitializedBufferedWriter(origin io.Writer) *bufferedWriter {
	bufWriter := bufio.NewWriter(origin)
	lbWriter := NewWriter(bufWriter)

	return NewBufferedWriter(lbWriter, bufWriter)
}

func NewBufferedWriter(origin LBWriter, flusher Flusher) *bufferedWriter {
	return &bufferedWriter{origin: origin, flusher: flusher}
}

type bufferedWriter struct {
	origin  LBWriter
	flusher Flusher
}

func (w *bufferedWriter) Write(data []byte) error {
	if err := w.origin.WriteLengthWithData(data); err != nil {
		return errors.Wrap(err, "write data failed")
	}
	return nil
}

func (w *bufferedWriter) Flush() error {
	return w.flusher.Flush()
}
