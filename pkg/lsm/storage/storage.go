package storage

import (
	"fmt"
	"sync"

	"github.com/atkhx/ddb/pkg/base"
	"github.com/atkhx/ddb/pkg/walog"
	"github.com/pkg/errors"
)

func New(memTable MemTable, memLength, memCapacity int, memFlusher MemFlusher, ssTables []SSTable, walogWriter WalogWriter) *storage {
	return &storage{
		memTable:    memTable,
		memFlusher:  memFlusher,
		ssTables:    ssTables,
		walogWriter: walogWriter,
		memCapacity: memCapacity,
		memLength:   memLength,
	}
}

type storage struct {
	sync.RWMutex

	memTable    MemTable
	memLength   int
	memCapacity int
	memFlusher  MemFlusher

	ssTables    []SSTable
	walogWriter WalogWriter
}

func (s *storage) Get(k base.Key) (Row, error) {
	s.RLock()
	defer s.RUnlock()

	r, err := s.searchInMemTable(k)
	if err != nil || r != nil {
		return r, err
	}

	for i := len(s.ssTables); i > 0; i-- {
		r, err := s.ssTables[i-1].Search(k)
		if err != nil {
			return nil, errors.Wrapf(err, "search in ssTable[%d] failed with error", i)
		}

		if r != nil {
			return r, nil
		}
	}

	return nil, nil
}

func (s *storage) searchInMemTable(k base.Key) (Row, error) {

	if s.memTable != nil {
		r, err := s.memTable.Search(k)
		if err != nil {
			return nil, errors.Wrap(err, "search in memTable failed with error")
		}

		if r != nil {
			return r, nil
		}
	}
	return nil, nil
}

func (s *storage) Set(row Row) error {
	s.Lock()
	defer s.Unlock()

	b, err := row.Serialize()
	if err != nil {
		return errors.Wrap(err, "serialize row failed")
	}

	if err := s.walogWriter.Write(walog.NewRecord(b)); err != nil {
		return errors.Wrap(err, "save to file store failed")
	}

	if err := s.memTable.Insert(row); err != nil {
		return errors.Wrap(err, "insert to memTable failed")
	}

	s.memLength += len(b)

	return s.flushMemTableIfNeed()
}

func (s *storage) Flush() error {
	s.Lock()
	defer s.Unlock()

	if s.memLength >= s.memCapacity {
		return s.flushMemTable()
	}
	return nil
}

func (s *storage) flushMemTableIfNeed() error {
	if s.memLength >= s.memCapacity {
		return s.flushMemTable()
	}
	return nil
}

func (s *storage) flushMemTable() error {
	if s.memLength > 0 {
		ssTable, err := s.memFlusher.Flush(s.memTable)
		if err != nil {
			return errors.Wrap(err, "flush table failed")
		}

		s.ssTables = append(s.ssTables, ssTable)

		if err := s.walogWriter.Flush(); err != nil {
			return errors.Wrap(err, "flush walog failed")
		}

		s.memTable.Reset()
		s.memLength = 0
	}
	return nil
}

func (s *storage) Close() error {
	for idx, ssTable := range s.ssTables {
		if err := ssTable.Close(); err != nil {
			fmt.Printf("close ssTable[%d]: %v\n", idx, ssTable.Close())
		}
	}
	return nil
}
