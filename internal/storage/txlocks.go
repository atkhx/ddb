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

	prev *txLock
	next *txLock

	right *txLock
	left  *txLock
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
	l.Lock()
	defer l.Unlock()

	return l.lockKey(txID, key)
}

func (l *txLocks) InitLocks(txID int64, keys ...internal.Key) ([]waitChan, error) {
	l.Lock()
	defer l.Unlock()

	waitChans := []waitChan{}

	for _, key := range keys {
		waitChan, err := l.lockKey(txID, key)
		if err != nil {
			return nil, err
		}

		if waitChan != nil {
			waitChans = append(waitChans, waitChan)
		}
	}

	return waitChans, nil
}

func (l *txLocks) createLock(txID int64, key internal.Key, needWait bool) *txLock {
	var lock *txLock
	if needWait {
		lock = NewTxLockWithWait(atomic.AddInt64(&l.maxLockId, 1), txID, key)
	} else {
		lock = NewTxLock(atomic.AddInt64(&l.maxLockId, 1), txID, key, nil)
	}

	if first, ok := l.locksByTxSingle[txID]; ok {
		lock.right = first
		lock.left = first.left
		first.left.right = lock
		first.left = lock
	} else {
		lock.right = lock
		lock.left = lock
	}
	l.locksByTxSingle[txID] = lock

	return lock
}

func (l *txLocks) lockKey(txID int64, key internal.Key) (waitChan, error) {
	lockByKey, ok := l.locksQueueSingle[key]
	if ok && lockByKey != nil {

		locker := lockByKey
		// исключаем самоблок по ключу
		for l := lockByKey; l != nil; l = l.next {
			if l.txID == txID {
				return nil, nil
			}

			locker = l
		}

		if curLock, ok := l.locksByTxSingle[txID]; ok {
			if err := l.isTxBlocksTargetByKeys(curLock, locker.txID, -1); err != nil {
				return nil, err
			}
		}

		lock := l.createLock(txID, key, true)
		lock.prev = locker
		locker.next = lock
		return lock.wait, nil
	}

	lock := l.createLock(txID, key, false)
	l.locksQueueSingle[key] = lock
	return nil, nil
}

func (l *txLocks) isTxBlocksTargetByKeys(curLock *txLock, targetTx int64, skipLockId int64) error {
	firstLockId := curLock.lockId
	firstLockIdChecked := false

	for checkLock := curLock; checkLock != nil; checkLock = checkLock.right {
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

		for targetLock := checkLock.next; targetLock != nil; targetLock = targetLock.next {
			// исходная транзакция блокирет целевую
			if targetLock.txID == targetTx {
				return ErrDeadLock
			}

			// проверяем вторичную блокировку
			// мы залочили targetLock.txID, а она могла залочить целевую
			if err := l.isTxBlocksTargetByKeys(targetLock.right, targetTx, targetLock.lockId); err != nil {
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

	for ; f != nil; f = f.right {
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

		if f.next != nil {
			f.next.prev = f.prev

			if f.prev == nil {
				f.next.wait <- true
				f.next.wait = nil
				l.locksQueueSingle[f.key] = f.next
			}
		} else if f.prev != nil {
			f.prev.next = f.next
		} else {
			delete(l.locksQueueSingle, f.key)
		}
	}
	delete(l.locksByTxSingle, txID)
}
