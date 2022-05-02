package storage

import (
	"testing"

	"github.com/atkhx/ddb/pkg/base/keys"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTxLocks_InitLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	tx3 := &txObj{txID: 3}

	key1 := keys.IntKey(1)
	key2 := keys.IntKey(2)
	key3 := keys.IntKey(3)

	waitFactory := NewMockTxLockWaitFactory(ctrl)

	t.Run("first transaction", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)
		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))
	})

	t.Run("second transaction with skip locked", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		waitFactory.EXPECT().Create().Return(make(waitChan, 1))
		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))

		err := txLocks.LockKey(tx2.GetID(), true, key1)
		assert.Error(t, err)
		assert.Equal(t, err, ErrSkipLocked)
	})

	t.Run("second transaction with wait for unlock", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		wait := make(waitChan, 1)
		wait <- true
		waitFactory.EXPECT().Create().Return(wait)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
	})

	t.Run("second transaction with wait cancelled", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		wait := make(waitChan, 1)
		wait <- false
		waitFactory.EXPECT().Create().Return(wait)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))

		err := txLocks.LockKey(tx2.GetID(), false, key1)
		assert.Error(t, err)
		assert.Equal(t, ErrWaitCancelled, err)
	})

	t.Run("no self deadlock", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
	})

	t.Run("deadlock 2tx", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		waitForTx2Key1 := make(waitChan, 1)
		waitForTx2Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx2Key1)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key2))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))

		assert.Equal(t, ErrDeadLock, txLocks.LockKey(tx1.GetID(), false, key2))
	})

	t.Run("deadlock 3tx", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		waitForTx2Key1 := make(waitChan, 1)
		waitForTx2Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx2Key1)

		waitForTx3Key2 := make(waitChan, 1)
		waitForTx3Key2 <- true
		waitFactory.EXPECT().Create().Return(waitForTx3Key2)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key2))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx3.GetID(), false, key3))
		assert.NoError(t, txLocks.LockKey(tx3.GetID(), false, key2))

		assert.Equal(t, ErrDeadLock, txLocks.LockKey(tx1.GetID(), false, key3))
		assert.Equal(t, ErrDeadLock, txLocks.LockKey(tx1.GetID(), false, key2))
	})
}

func TestTxLocks_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	tx3 := &txObj{txID: 3}

	key1 := keys.IntKey(1)
	key2 := keys.IntKey(2)

	waitFactory := NewMockTxLockWaitFactory(ctrl)

	t.Run("no locked keys", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)
		txLocks.Release(tx1.GetID())
	})

	t.Run("only one tx without waiting", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		assert.NoError(t, txLocks.LockKeys(tx1.GetID(), false, key1, key2))
		txLocks.Release(tx1.GetID())

		assert.Empty(t, txLocks.locksByTxSingle)
		assert.Empty(t, txLocks.locksQueueSingle)
	})

	t.Run("release first in chain", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		waitForTx2Key1 := make(waitChan, 1)
		waitForTx2Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx2Key1)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))

		txLocks.Release(tx1.GetID())

		assert.Nil(t, txLocks.locksByTxSingle[tx1.GetID()])
		assert.NotEmpty(t, txLocks.locksQueueSingle[key1])
		assert.Equal(t, tx2.GetID(), txLocks.locksQueueSingle[key1].txID)
	})

	t.Run("release middle in chain", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		waitForTx2Key1 := make(waitChan, 1)
		waitForTx2Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx2Key1)

		waitForTx3Key1 := make(waitChan, 1)
		waitForTx3Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx3Key1)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key2))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx3.GetID(), false, key1))

		txLocks.Release(tx2.GetID())

		assert.Nil(t, txLocks.locksByTxSingle[tx2.GetID()])
		assert.Empty(t, txLocks.locksQueueSingle[key2])
		assert.NotEmpty(t, txLocks.locksQueueSingle[key1])

		key1Locker := txLocks.locksQueueSingle[key1]

		assert.NotEmpty(t, key1Locker.nextInKeyQueue)
		assert.Equal(t, tx1.GetID(), key1Locker.txID)
		assert.Equal(t, tx3.GetID(), key1Locker.nextInKeyQueue.txID)

		assert.Nil(t, key1Locker.prevInKeyQueue)
	})

	t.Run("release last in chain", func(t *testing.T) {
		txLocks := NewTxLocks(waitFactory)

		waitForTx2Key1 := make(waitChan, 1)
		waitForTx2Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx2Key1)

		waitForTx3Key1 := make(waitChan, 1)
		waitForTx3Key1 <- true
		waitFactory.EXPECT().Create().Return(waitForTx3Key1)

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key2))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx3.GetID(), false, key1))

		txLocks.Release(tx3.GetID())

		assert.Nil(t, txLocks.locksByTxSingle[tx3.GetID()])
		assert.NotEmpty(t, txLocks.locksQueueSingle[key1])
		assert.NotEmpty(t, txLocks.locksQueueSingle[key2])

		key1Locker := txLocks.locksQueueSingle[key1]

		assert.NotNil(t, key1Locker.nextInKeyQueue)
		assert.Nil(t, key1Locker.nextInKeyQueue.nextInKeyQueue)

		assert.Equal(t, key1Locker, key1Locker.nextInKeyQueue.prevInKeyQueue)

		assert.Equal(t, tx1.GetID(), key1Locker.txID)
		assert.Equal(t, tx2.GetID(), key1Locker.nextInKeyQueue.txID)

		assert.Nil(t, key1Locker.prevInKeyQueue)
	})
}
