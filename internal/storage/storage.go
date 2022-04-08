package storage

import "errors"

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

func (s *storage) Begin(options ...txOpt) TxObj {
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

func (s *storage) Get(key Key) (Row, error) {
	txObj := s.Begin()
	defer func() {
		_ = s.Rollback(txObj)
	}()
	return s.TxGet(txObj, key)
}

func (s *storage) Set(key Key, row Row) error {
	txObj := s.Begin()
	if err := s.TxSet(txObj, key, row); err != nil {
		defer func() {
			_ = s.Rollback(txObj)
		}()
		return err
	}
	return s.Commit(txObj)
}

func (s *storage) TxGet(txObj TxObj, key Key) (Row, error) {
	row, err := s.txManager.Get(txObj, key)
	if err != nil || row != nil {
		return row, err
	}
	return s.roTables.Get(key)
}

func (s *storage) TxSet(txObj TxObj, key Key, row Row) error {
	if err := s.LockKeys(txObj, []Key{key}); err != nil {
		return err
	}
	return s.txManager.Set(txObj, key, row)
}

func (s *storage) TxGetForUpdate(txObj TxObj, key Key) (Row, error) {
	if err := s.LockKeys(txObj, []Key{key}); err != nil {
		return nil, err
	}

	row, err := s.TxGet(txObj, key)
	if err != nil {
		//s.txLocks.Release(txID)
		return nil, err
	}

	return row, err
}

func (s *storage) LockKeys(txObj TxObj, keys []Key) error {
	waitForUnlock, err := s.txLocks.InitLocks(txObj.GetID(), keys...)
	if err != nil {
		return err
	}

	if txObj.GetOptSkipLocked() && len(waitForUnlock) > 0 {
		return errors.New("already locked")
	}

	for _, wait := range waitForUnlock {
		if ok := <-wait; !ok {
			return errors.New("wait cancelled")
		}
	}
	return nil
}
