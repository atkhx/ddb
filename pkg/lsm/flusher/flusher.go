package flusher

import (
	"github.com/atkhx/ddb/pkg/lsm/sstable/filewriter"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/pkg/errors"
)

type SSConstructor interface {
	Create(filename string) (storage.SSTable, error)
}

type FileManager interface {
	CreateSSFileName() string
}

func New(ssConstructor SSConstructor, fileManager FileManager) *flusher {
	return &flusher{
		ssConstructor: ssConstructor,
		fileManager:   fileManager,
	}
}

type flusher struct {
	ssConstructor SSConstructor
	fileManager   FileManager
}

func (f *flusher) Flush(memTable storage.MemTable) (storage.SSTable, error) {
	filename := f.fileManager.CreateSSFileName()
	if err := f.saveMemTable(memTable, filename); err != nil {
		return nil, errors.Wrap(err, "save memTable failed")
	}
	return f.ssConstructor.Create(filename)
}

func (f *flusher) saveMemTable(memTable storage.MemTable, filename string) error {
	ssFileWriter, err := filewriter.OpenFile(filename)
	if err != nil {
		return errors.Wrapf(err, "open ssTable file '%s' failed", filename)
	}
	defer ssFileWriter.Close()
	defer ssFileWriter.Flush()

	return memTable.Scan(func(r storage.Row) (bool, error) {
		if err := ssFileWriter.Write(r); err != nil {
			return true, err
		}
		return false, nil
	})
}
