package storage

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/atkhx/ddb/internal"
)

var ErrDeadLock = errors.New("deadlock")

type waitChan chan bool

func NewTxLockWithWait(lockId, txID int64, key internal.Key) *txLock {
	return NewTxLock(lockId, txID, key, make(waitChan, 2))
}

func NewTxLock(lockId, txID int64, key internal.Key, wait waitChan) *txLock {
	return &txLock{
		lockId: lockId,
		wait:   wait,
		txID:   txID,
		key:    key,
	}
}

type txLock struct {
	lockId int64
	wait   chan bool
	txID   int64
	key    internal.Key

	prevInKeyQueue *txLock
	nextInKeyQueue *txLock

	prevInTx *txLock
	nextInTx *txLock
}

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

func (l *txLocks) InitLock(txID int64, key internal.Key) (waitChan, error) {
	return l.lockKey(txID, key)
}

func (l *txLocks) InitLocks(txID int64, keys ...internal.Key) (waitChans []waitChan, err error) {
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

func (l *txLocks) nextLockId() int64 {
	return atomic.AddInt64(&l.maxLockId, 1)
}

func (l *txLocks) createLock(lockId, txID int64, key internal.Key, needWait bool) *txLock {
	var lock *txLock
	if needWait {
		lock = NewTxLockWithWait(lockId, txID, key)
	} else {
		lock = NewTxLock(lockId, txID, key, nil)
	}

	if first, ok := l.locksByTxSingle[txID]; ok {
		lock.prevInTx = first
		lock.nextInTx = first.nextInTx
		first.nextInTx.prevInTx = lock
		first.nextInTx = lock
	} else {
		lock.prevInTx = lock
		lock.nextInTx = lock
	}
	l.locksByTxSingle[txID] = lock

	return lock
}

func (l *txLocks) lockKey(txID int64, key internal.Key) (waitChan, error) {
	l.Lock()
	defer l.Unlock()

	locker, ok := l.locksQueueSingle[key]
	if !ok || locker == nil {
		l.locksQueueSingle[key] = l.createLock(l.nextLockId(), txID, key, false)
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

	lock := l.createLock(l.nextLockId(), txID, key, true)
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
