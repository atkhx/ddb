package filescanner

import (
	"os"

	"github.com/atkhx/ddb/pkg/walog"
	"github.com/atkhx/ddb/pkg/walog/filereader"
	"github.com/pkg/errors"
)

func GetWalFiles(filesPath string) ([]string, error) {
	entities, err := os.ReadDir(filesPath)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entity := range entities {
		if !entity.IsDir() && len(entity.Name()) > 4 && ".wal" == entity.Name()[len(entity.Name())-4:] {
			files = append(files, filesPath+"/"+entity.Name())
		}
	}
	return files, nil
}

type fileScanner struct {
	files []string
}

func New(files []string) *fileScanner {
	return &fileScanner{files: files}
}

func (fs *fileScanner) Scan(callbackFn func(walog.Record) error) error {
	for _, filename := range fs.files {
		if err := fs.scanFile(filename, callbackFn); err != nil {
			return errors.Wrap(err, "scan file failed")
		}
	}
	return nil
}

func (fs *fileScanner) Clean() error {
	for _, filename := range fs.files {
		if err := os.Remove(filename); err != nil {
			return errors.Wrap(err, "remove file failed")
		}
	}
	return nil
}

func (fs *fileScanner) scanFile(filename string, callbackFn func(walog.Record) error) error {
	f, err := filereader.OpenFile(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Scan(callbackFn)
}
