package storage

import (
	"github.com/atkhx/ddb/pkg/base"
)

func NewStorage(
	roTables ROTables,
	txManager TxManager,
	txLocks Locks,
) *storage {
	return &storage{
		roTables:  roTables,
		txManager: txManager,
		txLocks:   txLocks,
	}
}

type storage struct {
	roTables  ROTables
	txManager TxManager
	txLocks   Locks
}

func (s *storage) Begin(options ...TxOpt) TxObj {
	return s.txManager.Begin(options...)
}

func (s *storage) Commit(txObj TxObj) error {
	defer s.txLocks.Release(txObj.GetID())
	return s.txManager.Commit(txObj)
}

func (s *storage) Rollback(txObj TxObj) error {
	defer s.txLocks.Release(txObj.GetID())
	return s.txManager.Rollback(txObj)
}

func (s *storage) Get(key base.Key) (interface{}, error) {
	txObj := s.Begin()
	defer func() {
		_ = s.Rollback(txObj)
	}()
	return s.TxGet(txObj, key)
}

func (s *storage) Set(key base.Key, row interface{}) error {
	txObj := s.Begin()
	if err := s.TxSet(txObj, key, row); err != nil {
		defer func() {
			_ = s.Rollback(txObj)
		}()
		return err
	}
	return s.Commit(txObj)
}

func (s *storage) TxGet(txObj TxObj, key base.Key) (interface{}, error) {
	row, err := s.txManager.Get(txObj, key)
	if err != nil || row != nil {
		return row, err
	}
	return s.roTables.Get(key)
}

func (s *storage) TxSet(txObj TxObj, key base.Key, row interface{}) error {
	if err := s.LockKey(txObj, key); err != nil {
		return err
	}
	return s.txManager.Set(txObj, key, row)
}

func (s *storage) TxGetForUpdate(txObj TxObj, key base.Key) (interface{}, error) {
	if err := s.LockKey(txObj, key); err != nil {
		return nil, err
	}

	row, err := s.TxGet(txObj, key)
	if err != nil {
		return nil, err
	}

	return row, err
}

func (s *storage) LockKey(txObj TxObj, key base.Key) error {
	return s.txLocks.LockKey(txObj.GetID(), txObj.GetIsolation().SkipLocked(), key)
}

func (s *storage) LockKeys(txObj TxObj, keys []base.Key) error {
	return s.txLocks.LockKeys(txObj.GetID(), txObj.GetIsolation().SkipLocked(), keys...)
}
