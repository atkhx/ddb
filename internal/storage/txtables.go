package storage

import (
	"errors"
	"sync"
)

var ErrNoWriteableTransaction = errors.New("no writeable transaction")

func NewTxTables(
	txAccess TxAccess,
	txFactory TxFactory,
	tabFactory RWTabFactory,
) *txTables {
	return &txTables{
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
		tx.commit()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txTables) Rollback(txID int64) error {
	if _, tx := tt.GetWriteable(txID); tx != nil {
		tx.rollback()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txTables) Persist(persistFn func(RWTable) error) {
	// iterate by copy
	tt.RLock()
	var tables = tt.tables
	tt.RUnlock()

	for _, txTable := range tables {
		if txTable.txObj.GetState() == TxCommitted {
			if err := persistFn(txTable.table); err != nil {
				break
			}
			txTable.txObj.persist()
		}
	}
}

func (tt *txTables) Vacuum() {
	tt.Lock()
	defer tt.Unlock()

	var tables []txTable
	for _, txTable := range tt.tables {
		if txTable.txObj.GetState() == TxRolledBack || txTable.txObj.GetState() == TxPersisted {
			continue
		}
		tables = append(tables, txTable)
	}

	tt.tables = tables
}

func (tt *txTables) GetWriteable(txID int64) (RWTable, TxObj) {
	// iterate by copy
	tt.RLock()
	var tables = tt.tables
	tt.RUnlock()

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
	tt.RLock()
	var tables = tt.tables
	tt.RUnlock()

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
