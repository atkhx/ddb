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

	txID := int64(123)

	txTables := NewMockTxTables(ctrl)
	txTables.EXPECT().Begin().Return(txID)

	storage := NewStorage(NewMockROTables(ctrl), txTables)
	assert.Equal(t, txID, storage.Begin())
}

func TestStorage_Commit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txID := int64(123)

	txTables := NewMockTxTables(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txTables)

	t.Run("success", func(t *testing.T) {
		txTables.EXPECT().Commit(txID).Return(nil)
		assert.NoError(t, storage.Commit(txID))
	})

	t.Run("fail", func(t *testing.T) {
		originErr := errors.New("some error")
		txTables.EXPECT().Commit(txID).Return(originErr)

		actualErr := storage.Commit(txID)

		assert.Error(t, actualErr)
		assert.Equal(t, originErr, actualErr)
	})
}

func TestStorage_Rollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txID := int64(123)

	txTables := NewMockTxTables(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txTables)

	t.Run("success", func(t *testing.T) {
		txTables.EXPECT().Rollback(txID).Return(nil)
		assert.NoError(t, storage.Rollback(txID))
	})

	t.Run("fail", func(t *testing.T) {
		originErr := errors.New("some error")
		txTables.EXPECT().Rollback(txID).Return(originErr)

		actualErr := storage.Rollback(txID)

		assert.Error(t, actualErr)
		assert.Equal(t, originErr, actualErr)
	})
}

func TestStorage_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txTables := NewMockTxTables(ctrl)
	roTables := NewMockROTables(ctrl)

	storage := NewStorage(roTables, txTables)

	t.Run("not found", func(t *testing.T) {
		txID := int64(123)
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Get(txID, 123).Return(nil, nil)
		txTables.EXPECT().Rollback(txID).Return(nil)

		roTables.EXPECT().Get(123).Return(nil, nil)

		row, err := storage.Get(123)
		assert.Nil(t, row)
		assert.NoError(t, err)
	})

	t.Run("error on rotables read", func(t *testing.T) {
		txID := int64(123)
		originErr := errors.New("some error")
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Get(txID, 123).Return(nil, nil)
		txTables.EXPECT().Rollback(txID).Return(nil)

		roTables.EXPECT().Get(123).Return(nil, originErr)

		row, err := storage.Get(123)
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("error on txtables read", func(t *testing.T) {
		txID := int64(123)
		originErr := errors.New("some error")
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Get(txID, 123).Return(nil, originErr)
		txTables.EXPECT().Rollback(txID).Return(nil)

		row, err := storage.Get(123)
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})

	t.Run("found in rotables read", func(t *testing.T) {
		txID := int64(123)
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Get(txID, 123).Return(nil, nil)
		txTables.EXPECT().Rollback(txID).Return(nil)

		roTables.EXPECT().Get(123).Return("some value", nil)

		row, err := storage.Get(123)
		assert.Equal(t, "some value", row)
		assert.NoError(t, err)
	})

	t.Run("found in txtables read", func(t *testing.T) {
		txID := int64(123)
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Get(txID, 123).Return("some value", nil)
		txTables.EXPECT().Rollback(txID).Return(nil)

		row, err := storage.Get(123)
		assert.Equal(t, "some value", row)
		assert.NoError(t, err)
	})
}

func TestStorage_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txTables := NewMockTxTables(ctrl)
	storage := NewStorage(NewMockROTables(ctrl), txTables)

	t.Run("success", func(t *testing.T) {
		txID := int64(3)
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Set(txID, 123, "some value").Return(nil)
		txTables.EXPECT().Commit(txID).Return(nil)

		err := storage.Set(123, "some value")
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		txID := int64(3)
		originErr := errors.New("some error")
		txTables.EXPECT().Begin().Return(txID)
		txTables.EXPECT().Set(txID, 123, "some value").Return(originErr)
		txTables.EXPECT().Rollback(txID).Return(nil)

		err := storage.Set(123, "some value")
		assert.Error(t, err)
		assert.Equal(t, originErr, err)
	})
}
