package storage

import (
	"errors"
)

var ErrNoWriteableTransaction = errors.New("no writeable txObj")

func NewTxManager(
	txFactory TxFactory,
	txTable RWTable,
) *txManager {
	return &txManager{
		txFactory: txFactory,
		txTable:   txTable,
	}
}

type txManager struct {
	txTable   RWTable
	txFactory TxFactory
}

func (tt *txManager) Begin(options ...TxOpt) TxObj {
	return tt.txFactory.Create(options...)
}

func (tt *txManager) Commit(txObj TxObj) error {
	if txObj.IsWriteable() {
		txObj.commit()
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

func (tt *txManager) Get(txObj TxObj, key Key) (row Row, err error) {
	txRows, err := tt.txTable.Get(key)
	if err != nil {
		return nil, err
	}

	if txRows == nil {
		return nil, nil
	}

	for i := len(txRows); i > 0; i-- {
		if txObj.GetIsolation().IsReadable(txRows[i-1].GetTxObj(), txObj) {
			return txRows[i-1].GetTxRow(), nil
		}
	}

	return nil, errors.New("row is not readable")
}

func (tt *txManager) Set(txObj TxObj, key Key, row Row) error {
	if txObj.IsWriteable() {
		return tt.txTable.Set(txObj, key, row)
	}
	return ErrNoWriteableTransaction
}
