package sorter_rows_slice

import (
	"sort"
	"sync"

	"github.com/atkhx/ddb/pkg/base"
	"github.com/atkhx/ddb/pkg/lsm/storage"
)

func NewItemsRowsSlice() *sortedRowsSlice {
	return &sortedRowsSlice{}
}

type sortedRowsSlice struct {
	sync.Mutex
	rows []storage.Row
}

func (r *sortedRowsSlice) Insert(row storage.Row) error {
	r.Lock()
	defer r.Unlock()

	idx, err := r.SearchIndex(row.Key())
	if err != nil {
		return err
	}

	if idx != -1 {
		r.rows[idx] = row
		return nil
	}

	r.rows = append(r.rows, row)

	sort.Slice(r.rows, func(i, j int) bool {
		return r.rows[i].Key().CompareWith(r.rows[j].Key()).IsLess()
	})

	return nil
}

func (r *sortedRowsSlice) Scan(fn func(storage.Row) (stop bool, err error)) error {
	for _, rr := range r.rows {
		stop, err := fn(rr)
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}
	return nil
}

func (r *sortedRowsSlice) SearchIndex(key base.Key) (int, error) {
	for idx, rr := range r.rows {
		if rr.Key().CompareWith(key).IsEqual() {
			return idx, nil
		}
	}
	return -1, nil
}

func (r *sortedRowsSlice) Search(key base.Key) (storage.Row, error) {
	idx, err := r.SearchIndex(key)
	if err != nil {
		return nil, err
	}

	if idx == -1 {
		return nil, nil
	}

	return r.rows[idx], nil
}

func (r *sortedRowsSlice) Reset() {
	r.Lock()
	defer r.Unlock()

	r.rows = []storage.Row{}
}
