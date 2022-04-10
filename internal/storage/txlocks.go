package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/atkhx/ddb/internal"
)

var ErrDeadLock = errors.New("deadlock")

type waitChan chan bool

func NewTxLockWithWait(txID int64, key internal.Key) *txLock {
	return NewTxLock(txID, key, make(waitChan, 2))
}

func NewTxLock(txID int64, key internal.Key, wait waitChan) *txLock {
	return &txLock{
		wait: wait,
		txID: txID,
		key:  key,
	}
}

type txLock struct {
	wait chan bool
	txID int64
	key  internal.Key
}

func (l *txLock) String() string {
	return fmt.Sprintf("ID:%d, KEY:%v", l.txID, l.key)
}

func NewTxLocks() *txLocks {
	return &txLocks{
		locksByTx:  map[int64][]*txLock{},
		locksQueue: map[internal.Key][]*txLock{},
	}
}

type txLocks struct {
	sync.RWMutex
	locksByTx  map[int64][]*txLock
	locksQueue map[internal.Key][]*txLock
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
	locks := l.locksQueue[key]
	if len(locks) > 0 {
		// исключаем самоблок по ключу
		for i := 0; i < len(locks); i++ {
			if locks[i].txID == txID {
				return nil, nil
			}
		}

		for _, currentTxLocker := range locks {
			if err := l.isTxBlocksTargetByKeys(txID, currentTxLocker.txID, nil); err != nil {
				return nil, err
			}
		}

		lock := NewTxLockWithWait(txID, key)
		l.locksQueue[key] = append(l.locksQueue[key], lock)
		l.locksByTx[txID] = append(l.locksByTx[txID], lock)
		return lock.wait, nil
	}

	lock := NewTxLock(txID, key, nil)
	l.locksQueue[key] = append(l.locksQueue[key], lock)
	l.locksByTx[txID] = append(l.locksByTx[txID], lock)
	return nil, nil
}

func (l *txLocks) isTxBlocksTargetByKeys(txID, targetTx int64, skipKey internal.Key) error {
	checkLocks := l.locksByTx[txID]
	for _, checkLock := range checkLocks {
		if skipKey == checkLock.key {
			continue
		}

		foundCheckLock := false
		for _, targetLock := range l.locksQueue[checkLock.key] {
			if !foundCheckLock {
				foundCheckLock = targetLock.txID == checkLock.txID
				continue
			}

			// далее только заблокированные нами (checkLock)

			// исходная транзакция блокирет целевую
			if targetLock.txID == targetTx {
				return ErrDeadLock
			}

			// проверяем вторичную блокировку
			// мы залочили targetLock.txID, а она могла залочить целевую
			if err := l.isTxBlocksTargetByKeys(targetLock.txID, targetTx, checkLock.key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *txLocks) getTxInQueueByKey(txID int64, key internal.Key) (int, *txLock) {
	for i := 0; i < len(l.locksQueue[key]); i++ {
		if l.locksQueue[key][i].txID == txID {
			return i, l.locksQueue[key][i]
		}
	}
	return 0, nil
}

func (l *txLocks) Release(txID int64) {
	l.Lock()
	defer l.Unlock()

	locksByTx := l.locksByTx[txID]

	for j := 0; j < len(locksByTx); j++ {
		f := *locksByTx[j]
		key := f.key

		i, ff := l.getTxInQueueByKey(txID, key)
		if ff == nil {
			panic("lock not found in queue")
		}

		if f.wait != nil {
			f.wait <- false
		}

		locksByKey := l.locksQueue[key]
		newLocks := []*txLock{}
		for k := 0; k < len(locksByKey); k++ {
			if k != i {
				newLocks = append(newLocks, locksByKey[k])
			}
		}

		if i == 0 && len(newLocks) > 0 {
			newLocks[0].wait <- true
			newLocks[0].wait = nil
		}

		l.locksQueue[key] = newLocks
	}

	delete(l.locksByTx, txID)
}
