package storage

import (
	"errors"
	"sync"
)

var ErrNoWriteableTransaction = errors.New("no writeable txObj")

func NewTxManager(
	txAccess TxAccess,
	txFactory TxFactory,
	tabFactory RWTabFactory,
) *txManager {
	return &txManager{
		txAccess:   txAccess,
		txFactory:  txFactory,
		tabFactory: tabFactory,
	}
}

type txChain struct {
	txObj TxObj
	next  *txChain
	prev  *txChain
}

type txManager struct {
	sync.RWMutex
	txAccess TxAccess
	txChain  *txChain

	txFactory  TxFactory
	tabFactory RWTabFactory
}

func (tt *txManager) Begin(options ...txOpt) TxObj {
	return tt.txFactory.Create(tt.tabFactory.Create(), options...)
}

func (tt *txManager) Commit(txObj TxObj) error {
	if tt.txAccess.IsWriteable(txObj) {
		tt.Lock()
		txObj.commit()

		if tt.txChain == nil {
			tt.txChain = &txChain{txObj: txObj}
		} else {
			tt.txChain.next = &txChain{txObj: txObj, prev: tt.txChain}
			tt.txChain = tt.txChain.next
		}
		tt.Unlock()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txManager) Rollback(txObj TxObj) error {
	if tt.txAccess.IsWriteable(txObj) {
		txObj.rollback()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txManager) Persist(persistFn func(RWTable) error) {
	tt.RLock()
	defer tt.RUnlock()
	//
	//var chain = tt.txChain
	//for chain != nil {
	//	if chain.txObj.GetState() == TxCommitted {
	//		if err := persistFn(chain.table); err != nil {
	//			break
	//		}
	//		chain.txObj.persist()
	//	}
	//	chain = chain.Next
	//}
}

func (tt *txManager) Vacuum() {
	tt.Lock()
	defer tt.Unlock()
	//
	//var tables []txTable
	//for _, txTable := range tt.tables {
	//	if txTable.txObj.GetState() == TxRolledBack || txTable.txObj.GetState() == TxPersisted {
	//		continue
	//	}
	//	tables = append(tables, txTable)
	//}
	//
	//tt.tables = tables
}

func (tt *txManager) IterateReadable(txObj TxObj, fn func(RWTable) bool) {
	if txObj.GetState() == TxUncommitted && tt.txAccess.IsReadable(txObj, txObj) && fn(txObj.getTxTable()) {
		tt.RUnlock()
		return
	}

	txt := tt.txChain
	for txt != nil {
		if tt.txAccess.IsReadable(txt.txObj, txObj) && fn(txt.txObj.getTxTable()) {
			return
		}
		txt = txt.prev
	}
	return
}

func (tt *txManager) Get(txObj TxObj, key Key) (row Row, err error) {
	tt.IterateReadable(txObj, func(table RWTable) bool {
		row, err = table.Get(key)
		return row != nil || err != nil
	})
	return
}

func (tt *txManager) Set(txObj TxObj, key Key, row Row) error {
	if tt.txAccess.IsWriteable(txObj) {
		return txObj.getTxTable().Set(key, row)
	}
	return ErrNoWriteableTransaction
}
