package constructor

import (
	"github.com/atkhx/ddb/pkg/lsm/sstable"
	"github.com/atkhx/ddb/pkg/lsm/sstable/filereader"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/pkg/errors"
)

type FileManager interface {
	GetFlushedTabFiles() ([]string, error)
}

type RowUnSerializer interface {
	RowFromBytes([]byte) (storage.Row, error)
}

func New(fileManager FileManager, rowUnSerializer RowUnSerializer) *constructor {
	return &constructor{fileManager: fileManager, rowUnSerializer: rowUnSerializer}
}

type constructor struct {
	fileManager     FileManager
	rowUnSerializer RowUnSerializer
}

func (c *constructor) Create(filename string) (storage.SSTable, error) {
	ssFileReader, err := filereader.OpenFile(filename, c.rowUnSerializer)
	if err != nil {
		return nil, errors.Wrap(err, "open ssFileReaderFile failed")
	}
	return sstable.NewSSTable(ssFileReader, nil), nil
}

func (c *constructor) InitSSTables() (ssTables []storage.SSTable, err error) {
	tabFiles, err := c.fileManager.GetFlushedTabFiles()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			for _, ssTable := range ssTables {
				_ = ssTable.Close()
			}
		}
	}()

	for _, tabFile := range tabFiles {
		ssTable, createSSTableErr := c.Create(tabFile)
		if createSSTableErr != nil {
			err = createSSTableErr
			break
		}
		ssTables = append(ssTables, ssTable)
	}
	return
}
