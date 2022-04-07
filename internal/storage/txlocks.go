package storage

import (
	"errors"
	"sync"
)

var ErrDeadLock = errors.New("deadlock")

type waitChan chan bool

func NewTxLockWithWait(txID int64, key Key) *txLock {
	return NewTxLock(txID, key, make(waitChan, 2))
}

func NewTxLock(txID int64, key Key, wait waitChan) *txLock {
	return &txLock{
		wait: wait,
		txID: txID,
		key:  key,
	}
}

type txLock struct {
	wait chan bool
	txID int64
	key  Key
}

func NewTxLocks() *txLocks {
	return &txLocks{
		locksByTx:  map[int64][]*txLock{},
		locksQueue: map[Key][]*txLock{},
	}
}

type txLocks struct {
	sync.RWMutex
	locksByTx  map[int64][]*txLock
	locksQueue map[Key][]*txLock
}

func (l *txLocks) InitLock(txID int64, key Key) (waitChan, error) {
	l.Lock()
	defer l.Unlock()

	return l.lockKey(txID, key)
}

func (l *txLocks) InitLocks(txID int64, keys ...Key) ([]waitChan, error) {
	l.Lock()
	defer l.Unlock()

	waitChans := []waitChan{}

	for _, key := range keys {
		waitChan, err := l.lockKey(txID, key)
		if err != nil {
			return nil, err
		}
		waitChans = append(waitChans, waitChan)
	}

	return waitChans, nil
}

func (l *txLocks) lockKey(txID int64, key Key) (waitChan, error) {
	locks := l.locksQueue[key]
	if len(locks) > 0 {
		// исключаем самоблок по ключу
		for i := 0; i < len(locks); i++ {
			if locks[i].txID == txID {
				return nil, nil
			}
		}

		for _, currentTxLocker := range locks {
			if err := l.isTxBlocksTargetByKeys(txID, currentTxLocker.txID, map[Key]Key{}); err != nil {
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

func (l *txLocks) isTxBlocksTargetByKeys(txID, targetTx int64, skipKeys map[Key]Key) error {
	checkLocks := l.locksByTx[txID]
	for _, checkLock := range checkLocks {
		if _, ok := skipKeys[checkLock.key]; ok {
			continue
		}

		foundCheckLock := false
		for _, targetLock := range l.locksQueue[checkLock.key] {

			if !foundCheckLock {
				if targetLock.txID == checkLock.txID {
					foundCheckLock = true
				}
				continue
			}

			// далее только заблокированные нами (checkLock)

			// исходная транзакция блокирет целевую
			if targetLock.txID == targetTx {
				return ErrDeadLock
			}

			// проверяем вторичную блокировку
			// мы залочили targetLock.txID, а она могла залочить целевую
			if err := l.isTxBlocksTargetByKeys(targetLock.txID, targetTx, map[Key]Key{checkLock.key: checkLock.key}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *txLocks) getTxInQueueByKey(txID int64, key Key) (int, *txLock) {
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

	for j := 0; j < len(l.locksByTx[txID]); j++ {
		f := *l.locksByTx[txID][j]
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
