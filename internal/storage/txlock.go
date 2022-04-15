package storage

import "github.com/atkhx/ddb/internal"

type waitChan chan bool

var waitFactory = func() waitChan {
	return make(waitChan, 1)
}

func NewTxLock(
	lockId int64,
	txID int64,
	key internal.Key,
	firstInTx *txLock,
	needWait bool,
) *txLock {
	lock := &txLock{
		lockId: lockId,
		txID:   txID,
		key:    key,
	}

	if needWait {
		lock.wait = waitFactory()
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
	wait   chan bool
	txID   int64
	key    internal.Key

	prevInKeyQueue *txLock
	nextInKeyQueue *txLock

	prevInTx *txLock
	nextInTx *txLock
}
