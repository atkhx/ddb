package storage

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/atkhx/ddb/internal"
)

var ErrDeadLock = errors.New("deadlock")

func NewTxLocks() *txLocks {
	return &txLocks{
		locksQueueSingle: map[internal.Key]*txLock{},
		locksByTxSingle:  map[int64]*txLock{},
	}
}

type txLocks struct {
	maxLockId int64
	sync.RWMutex
	locksByTxSingle  map[int64]*txLock
	locksQueueSingle map[internal.Key]*txLock
}

func (l *txLocks) LockKey(txID int64, key internal.Key) (waitChan, error) {
	return l.lockKey(txID, key)
}

func (l *txLocks) LockKeys(txID int64, keys ...internal.Key) (waitChans []waitChan, err error) {
	for _, key := range keys {
		waitChan, err := l.lockKey(txID, key)
		if err != nil {
			return nil, err
		}

		if waitChan != nil {
			waitChans = append(waitChans, waitChan)
		}
	}

	return
}

func (l *txLocks) createLock(txID int64, key internal.Key, needWait bool) *txLock {
	lock := NewTxLock(
		atomic.AddInt64(&l.maxLockId, 1),
		txID,
		key,
		l.locksByTxSingle[txID],
		needWait,
	)

	l.locksByTxSingle[txID] = lock
	return lock
}

func (l *txLocks) lockKey(txID int64, key internal.Key) (waitChan, error) {
	l.Lock()
	defer l.Unlock()

	locker := l.locksQueueSingle[key]
	if locker == nil {
		l.locksQueueSingle[key] = l.createLock(txID, key, false)
		return nil, nil
	}

	// исключаем самоблок по ключу
	// проматываем locker на последнюю блокировку ключа key
	for l := locker; l != nil; l = l.nextInKeyQueue {
		if l.txID == txID {
			return nil, nil
		}
		locker = l
	}

	if _, ok := l.locksByTxSingle[txID]; ok {
		if err := l.isTargetBlockedByTx(locker, txID, -1); err != nil {
			return nil, err
		}
	}

	lock := l.createLock(txID, key, true)
	lock.prevInKeyQueue = locker
	locker.nextInKeyQueue = lock
	return lock.wait, nil
}

func (l *txLocks) isTargetBlockedByTx(targetLock *txLock, tx, skipLockId int64) error {
	firstLockId := targetLock.lockId
	firstLockIdChecked := false

	for checkLock := targetLock; checkLock != nil; checkLock = checkLock.prevInTx {
		if firstLockId == checkLock.lockId {
			if !firstLockIdChecked {
				firstLockIdChecked = true
			} else {
				break
			}
		}

		if skipLockId == checkLock.lockId {
			continue
		}

		for curLock := checkLock.prevInKeyQueue; curLock != nil; curLock = curLock.prevInKeyQueue {
			if curLock.txID == tx {
				return ErrDeadLock
			}

			if err := l.isTargetBlockedByTx(curLock, tx, curLock.lockId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *txLocks) Release(txID int64) {
	l.Lock()
	defer l.Unlock()

	f, ok := l.locksByTxSingle[txID]
	if !ok {
		return
	}

	initLockId := l.locksByTxSingle[txID].lockId
	initLockIdCheck := false

	for ; f != nil; f = f.prevInTx {
		if initLockId == f.lockId {
			if !initLockIdCheck {
				initLockIdCheck = true
			} else {
				break
			}
		}
		if f.wait != nil {
			f.wait <- false
		}

		if f.nextInKeyQueue != nil {
			f.nextInKeyQueue.prevInKeyQueue = f.prevInKeyQueue

			if f.prevInKeyQueue == nil {
				f.nextInKeyQueue.wait <- true
				f.nextInKeyQueue.wait = nil
				l.locksQueueSingle[f.key] = f.nextInKeyQueue
			}
		} else if f.prevInKeyQueue != nil {
			f.prevInKeyQueue.nextInKeyQueue = f.nextInKeyQueue
		} else {
			delete(l.locksQueueSingle, f.key)
		}
	}
	delete(l.locksByTxSingle, txID)
}
