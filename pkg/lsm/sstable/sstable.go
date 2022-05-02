package sstable

import (
	"io"

	"github.com/atkhx/ddb/pkg/base"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/pkg/errors"
)

type SSReader interface {
	ReadRow() (int64, storage.Row, error)
	ReadRowAt(pos int64) (int64, storage.Row, error)
	Reset() error
	Close() error
}

type SSIndex interface {
	Search(key base.Key) (int64, error)
}

func NewSSTable(reader SSReader, index SSIndex) *ssTable {
	return &ssTable{reader: reader, index: index}
}

type ssTable struct {
	reader SSReader
	index  SSIndex
}

func (s *ssTable) Search(k base.Key) (storage.Row, error) {
	// search by index
	if s.index != nil {
		if p, err := s.index.Search(k); err != nil {
			return nil, err
		} else if p != -1 {
			_, r, err := s.reader.ReadRowAt(p)
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, errors.Wrap(err, "read row failed")
			}
			return r, nil
		}
	}

	// full scan
	if err := s.reader.Reset(); err != nil {
		return nil, err
	}

	for {
		_, r, err := s.reader.ReadRow()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, errors.Wrap(err, "read row failed")
		}

		if r == nil {
			break
		}

		if r.Key().CompareWith(k).IsEqual() {
			return r, nil
		}
	}

	return nil, nil
}

func (s *ssTable) Close() error {
	return s.reader.Close()
}
