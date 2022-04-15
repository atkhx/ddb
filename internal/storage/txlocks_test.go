package storage

import (
	"testing"

	"github.com/atkhx/ddb/internal/keys"
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

	originWaitFactory := waitFactory
	defer func() {
		waitFactory = originWaitFactory
	}()

	t.Run("first transaction", func(t *testing.T) {
		txLocks := NewTxLocks()
		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))
	})

	t.Run("second transaction with skip locked", func(t *testing.T) {
		txLocks := NewTxLocks()
		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))

		err := txLocks.LockKey(tx2.GetID(), true, key1)
		assert.Error(t, err)
		assert.Equal(t, err, ErrSkipLocked)
	})

	t.Run("second transaction with wait for unlock", func(t *testing.T) {
		txLocks := NewTxLocks()

		waitFactory = func() waitChan {
			wait := originWaitFactory()
			wait <- true
			return wait
		}

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
	})

	t.Run("second transaction with wait cancelled", func(t *testing.T) {
		txLocks := NewTxLocks()

		waitFactory = func() waitChan {
			wait := originWaitFactory()
			wait <- false
			return wait
		}

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), true, key1))

		err := txLocks.LockKey(tx2.GetID(), false, key1)
		assert.Error(t, err)
		assert.Equal(t, ErrWaitCancelled, err)
	})

	t.Run("no self deadlock", func(t *testing.T) {
		txLocks := NewTxLocks()

		waitFactory = func() waitChan {
			wait := originWaitFactory()
			wait <- true
			return wait
		}

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
	})

	t.Run("deadlock 2tx", func(t *testing.T) {
		txLocks := NewTxLocks()

		waitFactory = func() waitChan {
			wait := originWaitFactory()
			wait <- true
			return wait
		}

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key2))

		assert.Equal(t, ErrDeadLock, txLocks.LockKey(tx1.GetID(), false, key2))
	})

	t.Run("deadlock 3tx", func(t *testing.T) {
		txLocks := NewTxLocks()

		waitFactory = func() waitChan {
			wait := originWaitFactory()
			wait <- true
			return wait
		}

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key2))
		assert.NoError(t, txLocks.LockKey(tx3.GetID(), false, key2))
		assert.NoError(t, txLocks.LockKey(tx3.GetID(), false, key3))

		assert.Equal(t, ErrDeadLock, txLocks.LockKey(tx1.GetID(), false, key3))
		assert.Equal(t, ErrDeadLock, txLocks.LockKey(tx1.GetID(), false, key2))
	})
}

func TestTxLocks_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	//tx3 := &txObj{txID: 3}
	//
	key1 := keys.IntKey(1)
	key2 := keys.IntKey(2)
	//key3 := keys.IntKey(3)
	//

	originWaitFactory := waitFactory
	defer func() {
		waitFactory = originWaitFactory
	}()

	t.Run("no locked keys", func(t *testing.T) {
		txLocks := NewTxLocks()
		txLocks.Release(tx1.GetID())
	})

	t.Run("only one tx without waiting", func(t *testing.T) {
		txLocks := NewTxLocks()
		assert.NoError(t, txLocks.LockKeys(tx1.GetID(), false, key1, key2))
		txLocks.Release(tx1.GetID())

		assert.Empty(t, txLocks.locksByTxSingle)
		assert.Empty(t, txLocks.locksQueueSingle)
	})

	t.Run("release first in chain", func(t *testing.T) {
		txLocks := NewTxLocks()

		waitFactory = func() waitChan {
			wait := originWaitFactory()
			wait <- true
			return wait
		}

		assert.NoError(t, txLocks.LockKey(tx1.GetID(), false, key1))
		assert.NoError(t, txLocks.LockKey(tx2.GetID(), false, key1))

		txLocks.Release(tx1.GetID())

		assert.Nil(t, txLocks.locksByTxSingle[tx1.GetID()])
		//assert.Empty(t, txLocks.locksQueueSingle)

	})
}
