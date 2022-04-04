package storage

import (
	"errors"
	"sync"
)

var ErrNoWriteableTransaction = errors.New("no writeable transaction")

func NewTxTables(
	locks Locks,
	txAccess TxAccess,
	txFactory TxFactory,
	tabFactory RWTabFactory,
) *txTables {
	return &txTables{
		locks:      locks,
		txAccess:   txAccess,
		txFactory:  txFactory,
		tabFactory: tabFactory,
	}
}

type txTable struct {
	txObj TxObj
	table RWTable
}

type txTables struct {
	sync.RWMutex
	txAccess TxAccess
	tables   []txTable
	locks    Locks

	txFactory  TxFactory
	tabFactory RWTabFactory
}

func (tt *txTables) Begin() int64 {
	tt.Lock()
	defer tt.Unlock()

	txObj := tt.txFactory.Create()
	table := tt.tabFactory.Create()

	tt.tables = append(tt.tables, txTable{
		txObj: txObj,
		table: table,
	})

	return txObj.GetID()
}

func (tt *txTables) Commit(txID int64) error {
	if _, tx := tt.GetWriteable(txID); tx != nil {
		defer tt.locks.Release(tx.GetID())
		tx.commit()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txTables) Rollback(txID int64) error {
	if _, tx := tt.GetWriteable(txID); tx != nil {
		defer tt.locks.Release(tx.GetID())
		tx.rollback()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txTables) GetWriteable(txID int64) (RWTable, TxObj) {
	// iterate by copy
	var tables = tt.tables
	for i := len(tables); i > 0; i-- {
		if tables[i-1].txObj.GetID() > txID {
			continue
		}

		if tables[i-1].txObj.GetID() < txID {
			break
		}

		if tt.txAccess.IsWriteable(tables[i-1].txObj) {
			return tables[i-1].table, tables[i-1].txObj
		}

		break
	}
	return nil, nil
}

func (tt *txTables) IterateReadable(txID int64, fn func(RWTable) bool) {
	// iterate by copy
	var tables = tt.tables

	var tx TxObj
	for i := len(tables); i > 0; i-- {
		if tables[i-1].txObj.GetID() == txID {
			tx = tables[i-1].txObj
			break
		}
	}

	if tx == nil {
		return
	}

	for i := len(tables); i > 0; i-- {
		table := tables[i-1].table
		txObj := tables[i-1].txObj

		if tt.txAccess.IsReadable(txObj, tx) && fn(table) {
			return
		}
	}
	return
}

func (tt *txTables) Get(txID int64, key Key) (row Row, err error) {
	tt.IterateReadable(txID, func(table RWTable) bool {
		row, err = table.Get(key)
		return row != nil || err != nil
	})
	return
}

func (tt *txTables) Set(txID int64, key Key, row Row) error {
	if table, _ := tt.GetWriteable(txID); table != nil {
		return table.Set(key, row)
	}
	return ErrNoWriteableTransaction
}

func (tt *txTables) Upd(txID int64, key Key, row Row) error {
	table, tx := tt.GetWriteable(txID)
	if tx == nil {
		return ErrNoWriteableTransaction
	}

	waitForUnlock, err := tt.locks.InitLock(tx.GetID(), key)
	if err != nil {
		return err
	}

	if waitForUnlock != nil {
		<-waitForUnlock
	}

	return table.Set(key, row)
}
