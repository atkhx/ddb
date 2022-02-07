//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=filewriter_mocks.go
package filewriter

import (
	"io"
	"os"

	"github.com/atkhx/ddb/pkg/utils/lenbytewriter"
	"github.com/atkhx/ddb/pkg/walog"
	"github.com/atkhx/ddb/pkg/walog/writer"
)

type Closer interface {
	io.Closer
}

type Writer interface {
	Write(record walog.Record) (int, error)
}

func OpenFile(filename string) (*fileWriter, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	i, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &fileWriter{
		closer: f,
		writer: writer.NewWriter(lenbytewriter.NewInitializedBufferedWriter(f)),
		length: i.Size(),
	}, nil
}

func NewFileWriter(closer Closer, writer Writer) *fileWriter {
	return &fileWriter{
		closer: closer,
		writer: writer,
	}
}

type fileWriter struct {
	closer Closer
	writer Writer
	length int64
}

func (w *fileWriter) Length() int64 {
	return w.length
}

func (w *fileWriter) Write(record walog.Record) error {
	n, err := w.writer.Write(record)
	if err == nil {
		w.length += int64(n)
	}
	return err
}

func (w *fileWriter) Close() error {
	return w.closer.Close()
}
