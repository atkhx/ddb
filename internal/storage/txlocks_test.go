package storage

import (
	"testing"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/keys"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func assertSuccessLockWithoutWait(t *testing.T, locks *txLocks, tx TxObj, key internal.Key) {
	t.Helper()
	c, err := locks.LockKey(tx.GetID(), key)
	assert.Nil(t, c)
	assert.NoError(t, err)
}

func assertSuccessLockWithWaitChan(t *testing.T, locks *txLocks, tx TxObj, key internal.Key) waitChan {
	t.Helper()
	c, err := locks.LockKey(tx.GetID(), key)
	assert.NotNil(t, c)
	assert.NoError(t, err)
	return c
}

func assertLockWithDeadlockError(t *testing.T, locks *txLocks, tx TxObj, key internal.Key) {
	t.Helper()
	c, err := locks.LockKey(tx.GetID(), key)
	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Equal(t, ErrDeadLock, err)
}

func TestTxLocks_InitLock_SingleTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx := &txObj{txID: 1}
	txLocks := NewTxLocks()

	assertSuccessLockWithoutWait(t, txLocks, tx, keys.StrKey("key 1"))
	txLocks.Release(tx.GetID())
}

func TestTxLocks_InitLock_RepeatBySameTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx := &txObj{txID: 1}

	txLocks := NewTxLocks()

	assertSuccessLockWithoutWait(t, txLocks, tx, keys.StrKey("key 1"))
	assertSuccessLockWithoutWait(t, txLocks, tx, keys.StrKey("key 1"))

	txLocks.Release(tx.GetID())
}

func TestTxLocks_InitLock_WithWaitChan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}

	txLocks := NewTxLocks()

	assertSuccessLockWithoutWait(t, txLocks, tx1, keys.StrKey("key 1"))
	c := assertSuccessLockWithWaitChan(t, txLocks, tx2, keys.StrKey("key 1"))

	txLocks.Release(tx1.GetID())

	var lockReleased bool
	select {
	case lockReleased = <-c:
		assert.True(t, lockReleased)
	default:
	}
	assert.True(t, lockReleased)

	txLocks.Release(tx2.GetID())
}

func TestTxLocks_InitLock_WithDeadLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}

	txLocks := NewTxLocks()

	assertSuccessLockWithoutWait(t, txLocks, tx1, keys.StrKey("key 1"))
	assertSuccessLockWithoutWait(t, txLocks, tx2, keys.StrKey("key 2"))

	assertSuccessLockWithWaitChan(t, txLocks, tx1, keys.StrKey("key 2"))
	assertLockWithDeadlockError(t, txLocks, tx2, keys.StrKey("key 1"))
}

func TestTxLocks_InitLock_WithDeadLockChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	tx3 := &txObj{txID: 3}

	txLocks := NewTxLocks()

	assertSuccessLockWithoutWait(t, txLocks, tx1, keys.StrKey("key 1"))
	assertSuccessLockWithoutWait(t, txLocks, tx2, keys.StrKey("key 2"))
	assertSuccessLockWithoutWait(t, txLocks, tx3, keys.StrKey("key 3"))

	// tx2 заблокирована и ожидает tx1
	assertSuccessLockWithWaitChan(t, txLocks, tx2, keys.StrKey("key 1"))

	// tx3 заблокирована и ожидает tx2
	assertSuccessLockWithWaitChan(t, txLocks, tx3, keys.StrKey("key 2"))

	// tx1 получает deadlock: tx3 -> tx2 -> tx1
	assertLockWithDeadlockError(t, txLocks, tx1, keys.StrKey("key 3"))
}
