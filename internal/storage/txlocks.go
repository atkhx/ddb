package storage

import (
	"errors"
	"fmt"
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

	neighbor *txLock
}

func (l *txLock) String() string {
	return fmt.Sprintf("ID:%d, KEY:%v", l.txID, l.key)
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

		if err := l.isTxBlocksTargetByKeys(txID, locker.txID, locker.lockId); err != nil {
			return nil, err
		}

		lock := NewTxLockWithWait(atomic.AddInt64(&l.maxLockId, 1), txID, key)

		lock.prev = locker
		locker.next = lock

		if neighbor, ok := l.locksByTxSingle[txID]; ok {
			lock.neighbor = neighbor
		}

		l.locksByTxSingle[txID] = lock
		return lock.wait, nil
	}

	lock := NewTxLock(atomic.AddInt64(&l.maxLockId, 1), txID, key, nil)
	l.locksQueueSingle[key] = lock

	if neighbor, ok := l.locksByTxSingle[txID]; ok {
		lock.neighbor = neighbor
	}

	l.locksByTxSingle[txID] = lock

	return nil, nil
}

func (l *txLocks) isTxBlocksTargetByKeys(txID, targetTx int64, skipLockId int64) error {
	for checkLock := l.locksByTxSingle[txID]; checkLock != nil; checkLock = checkLock.neighbor {
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
			if err := l.isTxBlocksTargetByKeys(targetLock.txID, targetTx, targetLock.lockId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *txLocks) Release(txID int64) {
	l.Lock()
	defer l.Unlock()

	for f := l.locksByTxSingle[txID]; f != nil; f = f.neighbor {
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
