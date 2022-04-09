package storage

import (
	"time"

	"github.com/atkhx/ddb/pkg/localtime"
)

type TxState int

const (
	TxUncommitted TxState = iota
	TxRolledBack
	TxCommitted
	TxPersisted
)

func NewTxObj(txID int64, txTable RWTable, options ...txOpt) *txObj {
	res := &txObj{txID: txID}
	res.setState(TxUncommitted)
	res.txTable = txTable
	ReadCommitted()(res)

	for _, opt := range options {
		opt(res)
	}

	return res
}

type txObj struct {
	txID    int64
	txTime  time.Time
	txState TxState
	txTable RWTable

	txIsolation TxIsolation

	skipLocked bool
}

func (tx *txObj) GetID() int64 {
	return tx.txID
}

func (tx *txObj) GetState() TxState {
	return tx.txState
}

func (tx *txObj) GetTime() time.Time {
	return tx.txTime
}

func (tx *txObj) GetOptSkipLocked() bool {
	return tx.skipLocked
}

func (tx *txObj) IsWriteable() bool {
	return tx.txState == TxUncommitted
}

func (tx *txObj) IsReadable() bool {
	return tx.txState != TxRolledBack
}

func (tx *txObj) GetIsolation() TxIsolation {
	return tx.txIsolation
}

func (tx *txObj) setState(state TxState) {
	tx.txState = state
	tx.txTime = localtime.Now()
}

func (tx *txObj) getTxTable() RWTable {
	return tx.txTable
}

func (tx *txObj) commit() {
	tx.setState(TxCommitted)
}

func (tx *txObj) rollback() {
	tx.setState(TxRolledBack)
}

func (tx *txObj) persist() {
	tx.setState(TxPersisted)
}
