package storage

import (
	"testing"
	"time"

	"github.com/atkhx/ddb/pkg/localtime"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestTxTables_Begin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)

	txTables := NewTxManager(NewMockTxAccess(ctrl), txFactory, tabFactory)

	assert.Equal(t, tx1.GetID(), txTables.Begin())
	assert.Equal(t, tx2.GetID(), txTables.Begin())

	assert.Len(t, txTables.tables, 2)

	assert.Equal(t, tx1, txTables.tables[0].txObj)
	assert.Equal(t, tx2, txTables.tables[1].txObj)

	assert.Equal(t, tab1, txTables.tables[0].table)
	assert.Equal(t, tab2, txTables.tables[1].table)
}

func TestTxTables_Commit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)

	txAccess := NewMockTxAccess(ctrl)

	txTables := NewTxManager(txAccess, txFactory, tabFactory)

	txTables.Begin()
	txTables.Begin()

	t.Run("not existed transactionID", func(t *testing.T) {
		err := txTables.Commit(3)
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	t.Run("not writeable txObj", func(t *testing.T) {
		txAccess.EXPECT().IsWriteable(tx2).Return(false)
		err := txTables.Commit(2)
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	t.Run("success commit", func(t *testing.T) {
		txAccess.EXPECT().IsWriteable(tx1).Return(true)

		err := txTables.Commit(1)
		assert.NoError(t, err)
		assert.Equal(t, TxCommitted, tx1.txState)
		assert.Equal(t, now, tx1.txTime)
	})
}

func TestTxTables_Rollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)

	access := NewMockTxAccess(ctrl)

	txTables := NewTxManager(access, txFactory, tabFactory)

	txTables.Begin()
	txTables.Begin()

	t.Run("not existed transactionID", func(t *testing.T) {
		err := txTables.Rollback(3)
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	t.Run("not writeable txObj", func(t *testing.T) {
		access.EXPECT().IsWriteable(tx2).Return(false)
		err := txTables.Rollback(2)
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	t.Run("success rollback", func(t *testing.T) {
		access.EXPECT().IsWriteable(tx1).Return(true)

		err := txTables.Rollback(1)
		assert.NoError(t, err)
		assert.Equal(t, TxRolledBack, tx1.txState)
		assert.Equal(t, now, tx1.txTime)
	})
}

func TestTxTables_GetWriteable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)

	access := NewMockTxAccess(ctrl)

	txTables := NewTxManager(access, txFactory, tabFactory)

	txTables.Begin()
	txTables.Begin()

	t.Run("not existed transactionID", func(t *testing.T) {
		table, txObj := txTables.GetWriteable(3)
		assert.Nil(t, table)
		assert.Nil(t, txObj)
	})

	t.Run("not writeable txObj", func(t *testing.T) {
		access.EXPECT().IsWriteable(tx2).Return(false)

		table, txObj := txTables.GetWriteable(2)
		assert.Nil(t, table)
		assert.Nil(t, txObj)
	})

	t.Run("success rollback", func(t *testing.T) {
		access.EXPECT().IsWriteable(tx1).Return(true)

		table, txObj := txTables.GetWriteable(1)
		assert.Equal(t, tab1, table)
		assert.Equal(t, tx1, txObj)
	})
}

func TestTxTables_IterateReadable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	tx3 := &txObj{txID: 3}
	tx4 := &txObj{txID: 4}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)
	tab3 := NewMockRWTable(ctrl)
	tab4 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)
	txFactory.EXPECT().Create().Return(tx3).Times(1)
	txFactory.EXPECT().Create().Return(tx4).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)
	tabFactory.EXPECT().Create().Return(tab3).Times(1)
	tabFactory.EXPECT().Create().Return(tab4).Times(1)

	txAccess := NewMockTxAccess(ctrl)

	txTables := NewTxManager(txAccess, txFactory, tabFactory)

	t.Run("empty", func(t *testing.T) {
		var called bool
		txTables.IterateReadable(17, func(_ RWTable) bool {
			called = true
			return true
		})
		assert.False(t, called)
	})

	txTables.Begin()
	txTables.Begin()
	txTables.Begin()
	txTables.Begin()

	t.Run("not existed txObj", func(t *testing.T) {
		var called bool
		txTables.IterateReadable(17, func(_ RWTable) bool {
			called = true
			return true
		})
		assert.False(t, called)
	})

	t.Run("existed txObj", func(t *testing.T) {
		txAccess.EXPECT().IsReadable(tx2, tx2).Return(true)
		txAccess.EXPECT().IsReadable(tx3, tx2).Return(false)
		txAccess.EXPECT().IsReadable(tx4, tx2).Return(true)

		var calledTimes int
		txTables.IterateReadable(2, func(table RWTable) bool {
			calledTimes++

			if calledTimes == 1 {
				assert.Equal(t, tab4, table)
			}

			if calledTimes == 2 {
				assert.Equal(t, tab2, table)
				// break iterator
				return true
			}
			return false
		})

		assert.Equal(t, 2, calledTimes)
	})
}

func TestTxTables_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	tx3 := &txObj{txID: 3}
	tx4 := &txObj{txID: 4}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)
	tab3 := NewMockRWTable(ctrl)
	tab4 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)
	txFactory.EXPECT().Create().Return(tx3).Times(1)
	txFactory.EXPECT().Create().Return(tx4).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)
	tabFactory.EXPECT().Create().Return(tab3).Times(1)
	tabFactory.EXPECT().Create().Return(tab4).Times(1)

	txAccess := NewMockTxAccess(ctrl)

	txTables := NewTxManager(txAccess, txFactory, tabFactory)

	t.Run("empty", func(t *testing.T) {
		row, err := txTables.Get(17, 8910)
		assert.Nil(t, row)
		assert.NoError(t, err)
	})

	txTables.Begin()
	txTables.Begin()
	txTables.Begin()
	txTables.Begin()

	t.Run("not existed txObj", func(t *testing.T) {
		row, err := txTables.Get(18, 8910)
		assert.Nil(t, row)
		assert.NoError(t, err)
	})

	t.Run("existed txObj", func(t *testing.T) {
		txAccess.EXPECT().IsReadable(tx4, tx3).Return(false)
		txAccess.EXPECT().IsReadable(tx3, tx3).Return(true)
		txAccess.EXPECT().IsReadable(tx2, tx3).Return(true)

		tab3.EXPECT().Get(8910).Return(nil, nil)
		tab2.EXPECT().Get(8910).Return("some value", nil)

		row, err := txTables.Get(3, 8910)
		assert.Equal(t, "some value", row)
		assert.NoError(t, err)
	})

	t.Run("existed txObj", func(t *testing.T) {
		txAccess.EXPECT().IsReadable(tx4, tx3).Return(false)
		txAccess.EXPECT().IsReadable(tx3, tx3).Return(true)

		originErr := errors.New("some error")

		tab3.EXPECT().Get(8910).Return(nil, originErr)

		row, actualErr := txTables.Get(3, 8910)

		assert.Nil(t, row)
		assert.Error(t, actualErr)
		assert.EqualValues(t, originErr, actualErr)
	})
}

func TestTxTables_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	tx1 := &txObj{txID: 1}
	tx2 := &txObj{txID: 2}
	tx3 := &txObj{txID: 3}
	tx4 := &txObj{txID: 4}

	tab1 := NewMockRWTable(ctrl)
	tab2 := NewMockRWTable(ctrl)
	tab3 := NewMockRWTable(ctrl)
	tab4 := NewMockRWTable(ctrl)

	txFactory := NewMockTxFactory(ctrl)
	txFactory.EXPECT().Create().Return(tx1).Times(1)
	txFactory.EXPECT().Create().Return(tx2).Times(1)
	txFactory.EXPECT().Create().Return(tx3).Times(1)
	txFactory.EXPECT().Create().Return(tx4).Times(1)

	tabFactory := NewMockRWTabFactory(ctrl)
	tabFactory.EXPECT().Create().Return(tab1).Times(1)
	tabFactory.EXPECT().Create().Return(tab2).Times(1)
	tabFactory.EXPECT().Create().Return(tab3).Times(1)
	tabFactory.EXPECT().Create().Return(tab4).Times(1)

	txAccess := NewMockTxAccess(ctrl)

	txTables := NewTxManager(txAccess, txFactory, tabFactory)

	t.Run("empty", func(t *testing.T) {
		err := txTables.Set(17, 8910, "some value")
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	txTables.Begin()
	txTables.Begin()
	txTables.Begin()
	txTables.Begin()

	t.Run("not existed txObj", func(t *testing.T) {
		err := txTables.Set(18, 8910, "some value")
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	t.Run("txObj not writeable", func(t *testing.T) {
		txAccess.EXPECT().IsWriteable(tx3).Return(false)

		err := txTables.Set(3, 8910, "some value")
		assert.Error(t, err)
		assert.Equal(t, ErrNoWriteableTransaction, err)
	})

	t.Run("success set", func(t *testing.T) {
		txAccess.EXPECT().IsWriteable(tx2).Return(true)
		tab2.EXPECT().Set(8910, "some value").Return(nil)

		err := txTables.Set(2, 8910, "some value")
		assert.NoError(t, err)
	})

	t.Run("fail to set", func(t *testing.T) {
		originErr := errors.New("some error")

		txAccess.EXPECT().IsWriteable(tx1).Return(true)
		tab1.EXPECT().Set(8910, "some value").Return(originErr)

		actualErr := txTables.Set(1, 8910, "some value")
		assert.Error(t, actualErr)
		assert.Equal(t, originErr, actualErr)
	})
}
