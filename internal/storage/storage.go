package storage

import (
	"github.com/pkg/errors"
)

func NewStorage(
	roTables ROTables,
	txTables TxTables,
	txLocks Locks,
) *storage {
	return &storage{
		roTables: roTables,
		txTables: txTables,
		txLocks:  txLocks,
	}
}

type storage struct {
	roTables ROTables
	txTables TxTables
	txLocks  Locks
}

func (s *storage) Begin() int64 {
	return s.txTables.Begin()
}

func (s *storage) Commit(txID int64) error {
	defer s.txLocks.Release(txID)
	return s.txTables.Commit(txID)
}

func (s *storage) Rollback(txID int64) error {
	defer s.txLocks.Release(txID)
	return s.txTables.Rollback(txID)
}

func (s *storage) Get(key Key) (Row, error) {
	txID := s.Begin()
	defer func() {
		_ = s.Rollback(txID)
	}()
	return s.TxGet(txID, key)
}

func (s *storage) Set(key Key, row Row) error {
	txID := s.Begin()
	if err := s.TxSet(txID, key, row); err != nil {
		defer func() {
			_ = s.Rollback(txID)
		}()
		return err
	}
	return s.Commit(txID)
}

func (s *storage) TxGet(txID int64, key Key) (Row, error) {
	row, err := s.txTables.Get(txID, key)
	if err != nil || row != nil {
		return row, err
	}
	return s.roTables.Get(key)
}

func (s *storage) TxGetForUpdate(txID int64, skipLocked bool, key Key) (Row, error) {
	waitForUnlock, err := s.txLocks.InitLock(txID, key)
	if err != nil {
		return nil, err
	}

	if skipLocked && waitForUnlock != nil {
		return nil, errors.New("already locked")
	}

	if waitForUnlock != nil {
		if ok := <-waitForUnlock; !ok {
			return nil, errors.New("wait cancelled")
		}
	}

	row, err := s.TxGet(txID, key)
	if err != nil {
		//s.txLocks.Release(txID)
		return nil, err
	}

	return row, err
}

func (s *storage) LockKeys(txID int64, skipLocked bool, keys []Key) error {
	waitForUnlockChans, err := s.txLocks.InitLocks(txID, keys...)
	if err != nil {
		return err
	}

	if skipLocked && len(waitForUnlockChans) > 0 {
		return errors.New("already locked")
	}

	for _, waitForUnlock := range waitForUnlockChans {
		if ok := <-waitForUnlock; !ok {
			return errors.New("wait cancelled")
		}
	}
	return nil
}

func (s *storage) TxSet(txID int64, key Key, row Row) error {
	return s.txTables.Set(txID, key, row)
}
