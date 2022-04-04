package storage

import (
	"errors"
	"sync"
)

var ErrDeadLock = errors.New("deadlock")

type waitChan chan bool

func NewTxLock(txID int64) *txLock {
	return &txLock{
		txID: txID,
		wait: make(waitChan, 1),
	}
}

type txLock struct {
	wait chan bool
	txID int64
}

func NewTxLocks() *txLocks {
	return &txLocks{
		locksByKey: map[Key]int64{},
		locksByTx:  map[int64][]Key{},
		locksQueue: map[Key][]*txLock{},
	}
}

type txLocks struct {
	sync.RWMutex
	locksByKey map[Key]int64
	locksByTx  map[int64][]Key

	locksQueue map[Key][]*txLock
}

func (l *txLocks) InitLock(txID int64, key Key) (waitChan, error) {
	l.Lock()
	defer l.Unlock()

	i, alreadyLocked := l.locksByKey[key]
	if alreadyLocked {
		if i == txID {
			return nil, nil
		}

		if err := l.checkForDeadLock(txID, i); err != nil {
			return nil, err
		}

		lock := NewTxLock(txID)
		l.locksQueue[key] = append(l.locksQueue[key], lock)

		return lock.wait, nil
	}

	l.locksByKey[key] = txID
	l.locksByTx[txID] = append(l.locksByTx[txID], key)

	return nil, nil
}

func (l *txLocks) checkForDeadLock(txID int64, targetTxID int64) error {
	for key, locks := range l.locksQueue {
		for _, lock := range locks {
			if lock.txID == targetTxID {
				lockedByTx, ok := l.locksByKey[key]
				if !ok {
					continue
				}

				if lockedByTx == txID {
					return ErrDeadLock
				}

				if err := l.checkForDeadLock(txID, lockedByTx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (l *txLocks) Release(txID int64) {
	l.Lock()
	defer l.Unlock()

	release := []waitChan{}

	for _, key := range l.locksByTx[txID] {
		if chans, ok := l.locksQueue[key]; ok && len(chans) > 0 {
			lock := chans[0]
			l.locksQueue[key] = l.locksQueue[key][1:]

			release = append(release, lock.wait)
			l.locksByKey[key] = lock.txID
		} else {
			delete(l.locksByKey, key)
		}
	}

	delete(l.locksByTx, txID)

	for _, c := range release {
		c <- true
	}
}
