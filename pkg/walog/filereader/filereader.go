//go:generate mockgen -package=$GOPACKAGE -source=$GOFILE -destination=filereader_mocks.go
package filereader

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/atkhx/ddb/pkg/utils/lenbytereader"
	"github.com/atkhx/ddb/pkg/walog"
	"github.com/atkhx/ddb/pkg/walog/reader"
)

type Closer interface {
	io.Closer
}

type Reader interface {
	Read() (walog.Record, error)
}

func OpenFile(filename string) (*fileReader, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &fileReader{
		closer: f,
		reader: reader.NewReader(lenbytereader.NewReader(bufio.NewReader(f))),
	}, nil
}

func NewFileReader(closer Closer, reader Reader) *fileReader {
	return &fileReader{closer: closer, reader: reader}
}

type fileReader struct {
	closer Closer
	reader Reader
}

func (s *fileReader) Scan(callbackFn func(walog.Record) error) error {
	for {
		r, err := s.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if err := callbackFn(r); err != nil {
			return err
		}
	}

	return nil
}

func (s *fileReader) Close() error {
	return s.closer.Close()
}
