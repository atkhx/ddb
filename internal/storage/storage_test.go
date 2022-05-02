package storage

import (
	"errors"
	"testing"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/pkg/base/keys"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestStorage_Begin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txObj := NewTxObj(1)

	txManager := NewMockTxManager(ctrl)
	txManager.EXPECT().Begin().Return(txObj)

	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txManager, txLocks)
	assert.Equal(t, txObj, storage.Begin())
}

func TestStorage_Commit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txObj := NewTxObj(1)

	txManager := NewMockTxManager(ctrl)
	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txManager, txLocks)

	t.Run("success", func(t *testing.T) {
		txManager.EXPECT().Commit(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())
		assert.NoError(t, storage.Commit(txObj))
	})

	t.Run("fail", func(t *testing.T) {
		originErr := errors.New("some error")
		txManager.EXPECT().Commit(txObj).Return(originErr)
		txLocks.EXPECT().Release(txObj.GetID())

		actualErr := storage.Commit(txObj)

		assert.Error(t, actualErr)
		assert.Equal(t, originErr, actualErr)
	})
}

func TestStorage_Rollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txObj := NewTxObj(1)

	txManager := NewMockTxManager(ctrl)
	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txManager, txLocks)

	t.Run("success", func(t *testing.T) {
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())
		assert.NoError(t, storage.Rollback(txObj))
	})

	t.Run("fail", func(t *testing.T) {
		originErr := errors.New("some error")
		txManager.EXPECT().Rollback(txObj).Return(originErr)
		txLocks.EXPECT().Release(txObj.GetID())

		actualErr := storage.Rollback(txObj)

		assert.Error(t, actualErr)
		assert.Equal(t, originErr, actualErr)
	})
}

func TestStorage_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txManager := NewMockTxManager(ctrl)
	roTables := NewMockROTables(ctrl)
	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(roTables, txManager, txLocks)

	t.Run("not found", func(t *testing.T) {
		txObj := NewTxObj(1)
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, keys.IntKey(123)).Return(nil, nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		roTables.EXPECT().Get(keys.IntKey(123)).Return(nil, nil)

		row, err := storage.Get(keys.IntKey(123))
		assert.Nil(t, row)
		assert.NoError(t, err)
	})

	t.Run("error on rotables read", func(t *testing.T) {
		txObj := NewTxObj(123)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, keys.IntKey(123)).Return(nil, nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		roTables.EXPECT().Get(keys.IntKey(123)).Return(nil, originErr)

		row, err := storage.Get(keys.IntKey(123))
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("error on txManager read", func(t *testing.T) {
		txObj := NewTxObj(1)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, keys.IntKey(123)).Return(nil, originErr)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		row, err := storage.Get(keys.IntKey(123))
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("found in rotables read", func(t *testing.T) {
		txObj := NewTxObj(1)
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, keys.IntKey(123)).Return(nil, nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		roTables.EXPECT().Get(keys.IntKey(123)).Return("some value", nil)

		row, err := storage.Get(keys.IntKey(123))
		assert.Equal(t, "some value", row)
		assert.NoError(t, err)
	})

	t.Run("found in txManager read", func(t *testing.T) {
		txObj := NewTxObj(123)
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, keys.IntKey(123)).Return("some value", nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		row, err := storage.Get(keys.IntKey(123))
		assert.Equal(t, "some value", row)
		assert.NoError(t, err)
	})
}

func TestStorage_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txManager := NewMockTxManager(ctrl)
	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txManager, txLocks)

	t.Run("success", func(t *testing.T) {
		txObj := NewTxObj(1)
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Set(txObj, keys.IntKey(123), "some value").Return(nil)
		txManager.EXPECT().Commit(txObj).Return(nil)
		txLocks.EXPECT().LockKey(txObj.GetID(), false, keys.IntKey(123))
		txLocks.EXPECT().Release(txObj.GetID())

		err := storage.Set(keys.IntKey(123), "some value")
		assert.NoError(t, err)
	})

	t.Run("error on get row", func(t *testing.T) {
		txObj := NewTxObj(1)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Set(txObj, keys.IntKey(123), "some value").Return(originErr)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().LockKey(txObj.GetID(), false, keys.IntKey(123))
		txLocks.EXPECT().Release(txObj.GetID())

		err := storage.Set(keys.IntKey(123), "some value")
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("error on lock", func(t *testing.T) {
		txObj := NewTxObj(1)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().LockKey(txObj.GetID(), false, keys.IntKey(123)).Return(originErr)
		txLocks.EXPECT().Release(txObj.GetID())

		err := storage.Set(keys.IntKey(123), "some value")
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})
}

func TestStorage_TxGetForUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txManager := NewMockTxManager(ctrl)
	txLocks := NewMockLocks(ctrl)
	roTables := NewMockROTables(ctrl)
	storage := NewStorage(roTables, txManager, txLocks)

	t.Run("success without waiting", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)
		row := "some value"

		txLocks.EXPECT().LockKey(tx.txID, false, key).Return(nil)
		txManager.EXPECT().Get(tx, key).Return(row, nil)

		actualRow, actualErr := storage.TxGetForUpdate(tx, key)

		assert.NoError(t, actualErr)
		assert.Equal(t, row, actualRow)
	})

	t.Run("success on get from ROTables", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)
		row := "some value"

		txLocks.EXPECT().LockKey(tx.txID, false, key).Return(nil)
		txManager.EXPECT().Get(tx, key).Return(nil, nil)
		roTables.EXPECT().Get(key).Return(row, nil)

		actualRow, actualErr := storage.TxGetForUpdate(tx, key)

		assert.NoError(t, actualErr)
		assert.Equal(t, row, actualRow)
	})

	t.Run("row not found", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)

		txLocks.EXPECT().LockKey(tx.txID, false, key).Return(nil)
		txManager.EXPECT().Get(tx, key).Return(nil, nil)
		roTables.EXPECT().Get(key).Return(nil, nil)

		actualRow, actualErr := storage.TxGetForUpdate(tx, key)

		assert.NoError(t, actualErr)
		assert.Nil(t, actualRow)
	})

	t.Run("lock failed", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)
		err := errors.New("some error")

		txLocks.EXPECT().LockKey(tx.txID, false, key).Return(err)

		actualRow, actualErr := storage.TxGetForUpdate(tx, key)

		assert.Error(t, actualErr)
		assert.Equal(t, err, actualErr)
		assert.Nil(t, actualRow)
	})

	t.Run("fail to get row", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)
		err := errors.New("some get row error")

		txLocks.EXPECT().LockKey(tx.txID, false, key).Return(nil)
		txManager.EXPECT().Get(tx, key).Return(nil, err)

		actualRow, actualErr := storage.TxGetForUpdate(tx, key)

		assert.Error(t, actualErr)
		assert.EqualValues(t, err, actualErr)
		assert.Nil(t, actualRow)
	})
}

func TestStorage_LockKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), NewMockTxManager(ctrl), txLocks)

	t.Run("success lock", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)

		txLocks.EXPECT().LockKey(tx.GetID(), false, key).Return(nil)
		assert.NoError(t, storage.LockKey(tx, key))
	})

	t.Run("fail lock", func(t *testing.T) {
		tx := NewTxObj(1)
		key := keys.IntKey(2)
		err := errors.New("some error")

		txLocks.EXPECT().LockKey(tx.GetID(), false, key).Return(err)
		actualErr := storage.LockKey(tx, key)
		assert.Error(t, actualErr)
		assert.Equal(t, err, actualErr)
	})
}

func TestStorage_LockKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txLocks := NewMockLocks(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), NewMockTxManager(ctrl), txLocks)

	t.Run("success lock", func(t *testing.T) {
		tx := NewTxObj(1)
		key1 := keys.IntKey(2)
		key2 := keys.IntKey(3)

		txLocks.EXPECT().LockKeys(tx.GetID(), false, key1, key2).Return(nil)
		assert.NoError(t, storage.LockKeys(tx, []internal.Key{key1, key2}))
	})

	t.Run("fail lock", func(t *testing.T) {
		tx := NewTxObj(1)
		key1 := keys.IntKey(2)
		key2 := keys.IntKey(3)
		err := errors.New("some error")

		txLocks.EXPECT().LockKeys(tx.GetID(), false, key1, key2).Return(err)

		actualErr := storage.LockKeys(tx, []internal.Key{key1, key2})
		assert.Error(t, actualErr)
		assert.Equal(t, err, actualErr)
	})
}
