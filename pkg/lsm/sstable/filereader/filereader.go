package filereader

import (
	"bufio"
	"io"
	"os"

	"github.com/atkhx/ddb/pkg/lsm/sstable/reader"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/atkhx/ddb/pkg/utils/lenbytereader"
)

func OpenFile1(filename string) (*os.File, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return f, nil
}

type RowFile interface {
	io.Seeker
	io.Closer
}

type RowUnSerializer interface {
	RowFromBytes([]byte) (storage.Row, error)
}

type RowReader interface {
	ReadRow() (int64, storage.Row, error)
}

func OpenFile(filename string, rowUnserializer RowUnSerializer) (*fileReader, error) {
	ssFile, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	//ssFile, err := OpenFile1(filename)
	//if err != nil {
	//	return nil, errors.Wrap(err, "open ssFile failed")
	//}

	ssBuffer := bufio.NewReader(ssFile)
	rowReader := reader.NewReader(lenbytereader.NewReader(ssBuffer), rowUnserializer)
	ssReader := New(ssFile, rowReader)

	return ssReader, nil
}

func New(file RowFile, rowReader RowReader) *fileReader {
	return &fileReader{
		file:   file,
		reader: rowReader,
	}
}

type fileReader struct {
	file   RowFile
	reader RowReader
}

func (fr *fileReader) ReadRow() (int64, storage.Row, error) {
	return fr.reader.ReadRow()
}

func (fr *fileReader) ReadRowAt(pos int64) (int64, storage.Row, error) {
	if _, err := fr.file.Seek(pos, 0); err != nil {
		return 0, nil, err
	}

	return fr.ReadRow()
}

func (fr *fileReader) Reset() error {
	_, err := fr.file.Seek(0, io.SeekStart)
	return err
}

func (fr *fileReader) Close() error {
	return fr.file.Close()
}
