package tree

import (
	"github.com/atkhx/ddb/pkg/key"
	"github.com/atkhx/ddb/pkg/lsm/storage"
)

func NewTree(storage Storage) *tree {
	return &tree{
		storage: storage,
	}
}

type Storage interface {
	Get(k key.Key) (storage.Row, error)
	Set(r storage.Row) error
}

type tree struct {
	storage Storage
}

func (t *tree) Search(k key.Key) (storage.Row, error) {
	r, err := t.storage.Get(k)
	if err != nil {
		return nil, err
	}

	if r != nil && r.IsDeleted() {
		return nil, nil
	}
	return r, nil
}

func (t *tree) Insert(r storage.Row) error {
	return t.storage.Set(r)
}

func (t *tree) Delete(k key.Key) error {
	// todo просто сохранять удаляющую запись

	r, err := t.Search(k)
	if err != nil {
		return err
	}
	if r == nil {
		return nil
	}

	r.MakeDeleted()
	return t.storage.Set(r)
}
