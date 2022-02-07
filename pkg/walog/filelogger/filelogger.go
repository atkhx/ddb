package filelogger

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/atkhx/ddb/pkg/walog"
	"github.com/atkhx/ddb/pkg/walog/filewriter"
)

type FileWriter interface {
	Write(record walog.Record) error
	Length() int64
	Close() error
}

type logger struct {
	sync.RWMutex

	filesPath    string
	fileWriter   FileWriter
	fileCapacity int64

	filesForFlush []string
}

func New(filesPath string, fileCapacity int64) *logger {
	return &logger{
		filesPath:    filesPath,
		fileCapacity: fileCapacity,
	}
}

func (l *logger) Capacity() int64 {
	return l.fileCapacity
}

func (l *logger) Flush() error {
	l.Lock()
	defer l.Unlock()

	if l.fileWriter != nil {
		if err := l.fileWriter.Close(); err != nil {
			return err
		}
		l.fileWriter = nil
	}

	for i, filename := range l.filesForFlush {
		if err := os.Remove(filename); err != nil {
			l.filesForFlush = l.filesForFlush[i:]
			return err
		}
	}

	l.filesForFlush = []string{}
	return nil
}

func (l *logger) openFile() error {
	filename := fmt.Sprintf("%s/%d.wal", l.filesPath, time.Now().UnixNano())
	f, err := filewriter.OpenFile(filename)
	if err != nil {
		return err
	}

	l.filesForFlush = append(l.filesForFlush, filename)
	l.fileWriter = f
	return nil
}

func (l *logger) Write(record walog.Record) error {
	l.Lock()
	defer l.Unlock()

	if l.fileWriter != nil && l.fileWriter.Length() > l.fileCapacity {
		if err := l.fileWriter.Close(); err != nil {
			return err
		}
		l.fileWriter = nil
	}

	if l.fileWriter == nil {
		if err := l.openFile(); err != nil {
			return err
		}
	}

	return l.fileWriter.Write(record)
}

func (l *logger) Close() error {
	if l.fileWriter != nil {
		return l.fileWriter.Close()
	}
	return nil
}
