package filewriter

import (
	"io"
	"os"

	"github.com/atkhx/ddb/pkg/lsm/sstable/writer"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/atkhx/ddb/pkg/utils/lenbytewriter"
)

type Closer interface {
	io.Closer
}

type Flusher interface {
	Flush() error
}

type Writer interface {
	Write(row storage.Row) error
}

func OpenFile(filename string) (*fileWriter, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	ssBuffer := lenbytewriter.NewInitializedBufferedWriter(f)

	return New(f, writer.NewWriter(ssBuffer), ssBuffer), nil
}

func New(closer Closer, writer Writer, flusher Flusher) *fileWriter {
	return &fileWriter{
		closer:  closer,
		writer:  writer,
		flusher: flusher,
	}
}

type fileWriter struct {
	closer  Closer
	writer  Writer
	flusher Flusher
}

func (w *fileWriter) Write(row storage.Row) error {
	return w.writer.Write(row)
}

func (w *fileWriter) Flush() error {
	return w.flusher.Flush()
}

func (w *fileWriter) Close() error {
	return w.closer.Close()
}
