package filemanager

import (
	"fmt"
	"os"
	"time"
)

func New(rootPath string) *fileManager {
	return &fileManager{
		rootPath: rootPath,
	}
}

type fileManager struct {
	rootPath string
}

func (f *fileManager) getRootPath() string {
	return f.rootPath
}

func (f *fileManager) CreateSSFileName() string {
	return fmt.Sprintf("%s/%d.ss", f.rootPath, time.Now().UnixNano())
}

func (f *fileManager) GetFlushedTabFiles() ([]string, error) {
	entities, err := os.ReadDir(f.getRootPath())
	if err != nil {
		return nil, err
	}

	var files []string

	for _, entity := range entities {
		if !entity.IsDir() && len(entity.Name()) > 3 && ".ss" == entity.Name()[len(entity.Name())-3:] {
			files = append(files, f.rootPath+"/"+entity.Name())
		}
	}
	return files, nil
}
