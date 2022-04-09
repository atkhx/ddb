package storage

import (
	"errors"
	"testing"

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
		txManager.EXPECT().Get(txObj, 123).Return(nil, nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		roTables.EXPECT().Get(123).Return(nil, nil)

		row, err := storage.Get(123)
		assert.Nil(t, row)
		assert.NoError(t, err)
	})

	t.Run("error on rotables read", func(t *testing.T) {
		txObj := NewTxObj(123)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, 123).Return(nil, nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		roTables.EXPECT().Get(123).Return(nil, originErr)

		row, err := storage.Get(123)
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("error on txManager read", func(t *testing.T) {
		txObj := NewTxObj(1)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, 123).Return(nil, originErr)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		row, err := storage.Get(123)
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("found in rotables read", func(t *testing.T) {
		txObj := NewTxObj(1)
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, 123).Return(nil, nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		roTables.EXPECT().Get(123).Return("some value", nil)

		row, err := storage.Get(123)
		assert.Equal(t, "some value", row)
		assert.NoError(t, err)
	})

	t.Run("found in txManager read", func(t *testing.T) {
		txObj := NewTxObj(123)
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Get(txObj, 123).Return("some value", nil)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().Release(txObj.GetID())

		row, err := storage.Get(123)
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
		txManager.EXPECT().Set(txObj, 123, "some value").Return(nil)
		txManager.EXPECT().Commit(txObj).Return(nil)
		txLocks.EXPECT().InitLocks(txObj.GetID(), 123)
		txLocks.EXPECT().Release(txObj.GetID())

		err := storage.Set(123, "some value")
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		txObj := NewTxObj(1)
		originErr := errors.New("some error")
		txManager.EXPECT().Begin().Return(txObj)
		txManager.EXPECT().Set(txObj, 123, "some value").Return(originErr)
		txManager.EXPECT().Rollback(txObj).Return(nil)
		txLocks.EXPECT().InitLocks(txObj.GetID(), 123)
		txLocks.EXPECT().Release(txObj.GetID())

		err := storage.Set(123, "some value")
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})
}
