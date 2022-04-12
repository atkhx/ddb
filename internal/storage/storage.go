package storage

import (
	"errors"

	"github.com/atkhx/ddb/internal"
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

func (s *storage) Get(key internal.Key) (internal.Row, error) {
	txObj := s.Begin()
	defer func() {
		_ = s.Rollback(txObj)
	}()
	return s.TxGet(txObj, key)
}

func (s *storage) Set(key internal.Key, row internal.Row) error {
	txObj := s.Begin()
	if err := s.TxSet(txObj, key, row); err != nil {
		defer func() {
			_ = s.Rollback(txObj)
		}()
		return err
	}
	return s.Commit(txObj)
}

func (s *storage) TxGet(txObj TxObj, key internal.Key) (internal.Row, error) {
	row, err := s.txManager.Get(txObj, key)
	if err != nil || row != nil {
		return row, err
	}
	return s.roTables.Get(key)
}

func (s *storage) TxSet(txObj TxObj, key internal.Key, row internal.Row) error {
	if err := s.LockKey(txObj, key); err != nil {
		return err
	}
	return s.txManager.Set(txObj, key, row)
}

func (s *storage) TxGetForUpdate(txObj TxObj, key internal.Key) (internal.Row, error) {
	if err := s.LockKey(txObj, key); err != nil {
		return nil, err
	}

	row, err := s.TxGet(txObj, key)
	if err != nil {
		return nil, err
	}

	return row, err
}

func (s *storage) LockKey(txObj TxObj, key internal.Key) error {
	waitForUnlock, err := s.txLocks.InitLock(txObj.GetID(), key)
	if err != nil {
		return err
	}

	if waitForUnlock != nil {
		if txObj.GetIsolation().SkipLocked() {
			return errors.New("already locked")
		}

		if ok := <-waitForUnlock; !ok {
			return errors.New("wait cancelled")
		}
	}
	return nil
}

func (s *storage) LockKeys(txObj TxObj, keys []internal.Key) error {
	waitForUnlock, err := s.txLocks.InitLocks(txObj.GetID(), keys...)
	if err != nil {
		return err
	}

	if txObj.GetIsolation().SkipLocked() && len(waitForUnlock) > 0 {
		return errors.New("already locked")
	}

	for _, wait := range waitForUnlock {
		if ok := <-wait; !ok {
			return errors.New("wait cancelled")
		}
	}
	return nil
}
