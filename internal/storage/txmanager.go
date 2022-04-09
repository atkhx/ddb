package storage

import (
	"errors"
	"sync"
)

var ErrNoWriteableTransaction = errors.New("no writeable txObj")

func NewTxManager(
	txFactory TxFactory,
	tabFactory RWTabFactory,
) *txManager {
	return &txManager{
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
	txChain *txChain

	txFactory  TxFactory
	tabFactory RWTabFactory
}

func (tt *txManager) Begin(options ...txOpt) TxObj {
	return tt.txFactory.Create(tt.tabFactory.Create(), options...)
}

func (tt *txManager) Commit(txObj TxObj) error {
	if txObj.IsWriteable() {
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
	if txObj.IsWriteable() {
		txObj.rollback()
		return nil
	}
	return ErrNoWriteableTransaction
}

func (tt *txManager) IterateReadable(txObj TxObj, fn func(TxObj, RWTable) bool) {
	if fn(txObj, txObj.getTxTable()) {
		return
	}

	txt := tt.txChain
	for txt != nil {
		if fn(txt.txObj, txt.txObj.getTxTable()) {
			return
		}
		txt = txt.prev
	}
	return
}

func (tt *txManager) Get(txObj TxObj, key Key) (row Row, err error) {
	tt.IterateReadable(txObj, func(txt TxObj, table RWTable) bool {
		row, err = table.Get(key)
		if err != nil {
			return true
		}
		if row == nil {
			return false
		}
		if !txObj.GetIsolation().IsReadable(txt, txObj) {
			err = errors.New("row is not readable")
		}
		return true
	})
	return
}

func (tt *txManager) Set(txObj TxObj, key Key, row Row) error {
	if txObj.IsWriteable() {
		return txObj.getTxTable().Set(key, row)
	}
	return ErrNoWriteableTransaction
}
