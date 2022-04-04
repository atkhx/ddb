package storage

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestSsTables_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ssTables := NewSSTables()

	t.Run("get on empty", func(t *testing.T) {
		row, err := ssTables.Get(123)
		assert.NoError(t, err)
		assert.Nil(t, row)
	})

	tab0 := NewMockROTables(ctrl)
	tab1 := NewMockROTables(ctrl)
	tab2 := NewMockROTables(ctrl)

	ssTables.Grow(tab0)
	ssTables.Grow(tab1)
	ssTables.Grow(tab2)

	t.Run("not found", func(t *testing.T) {
		tab0.EXPECT().Get(123).Return(nil, nil)
		tab1.EXPECT().Get(123).Return(nil, nil)
		tab2.EXPECT().Get(123).Return(nil, nil)

		row, err := ssTables.Get(123)
		assert.NoError(t, err)
		assert.Nil(t, row)
	})

	t.Run("error on second tab", func(t *testing.T) {
		errOrigin := errors.New("some error")
		tab1.EXPECT().Get(234).Return(nil, errOrigin)
		tab2.EXPECT().Get(234).Return(nil, nil)

		row, err := ssTables.Get(234)
		assert.Nil(t, row)
		assert.Error(t, err)
		assert.Equal(t, errOrigin, err)
	})

	t.Run("get first value from old tab", func(t *testing.T) {
		tab1.EXPECT().Get(1).Return("some value", nil)
		tab2.EXPECT().Get(1).Return(nil, nil)

		row, err := ssTables.Get(1)
		assert.NoError(t, err)
		assert.Equal(t, "some value", row)
	})

	t.Run("get fresh value from new tab", func(t *testing.T) {
		tab2.EXPECT().Get(1).Return("some value", nil)

		row, err := ssTables.Get(1)
		assert.NoError(t, err)
		assert.Equal(t, "some value", row)
	})
}
