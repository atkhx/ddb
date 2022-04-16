package storage

import "github.com/atkhx/ddb/internal"

type waitChan chan bool

func NewTxLock(
	lockId int64,
	txID int64,
	key internal.Key,
	firstInTx *txLock,
	wait waitChan,
) *txLock {
	lock := &txLock{
		lockId: lockId,
		txID:   txID,
		key:    key,
		wait:   wait,
	}

	if firstInTx != nil {
		lock.prevInTx = firstInTx
		lock.nextInTx = firstInTx.nextInTx
		firstInTx.nextInTx.prevInTx = lock
		firstInTx.nextInTx = lock
	} else {
		lock.prevInTx = lock
		lock.nextInTx = lock
	}

	return lock
}

type txLock struct {
	lockId int64
	wait   waitChan
	txID   int64
	key    internal.Key

	prevInKeyQueue *txLock
	nextInKeyQueue *txLock

	prevInTx *txLock
	nextInTx *txLock
}
