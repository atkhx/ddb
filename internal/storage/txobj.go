package storage

import (
	"sync"
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

func NewTxObj(txID int64) *txObj {
	res := &txObj{txID: txID}
	res.setState(TxUncommitted)
	return res
}

type txObj struct {
	mu sync.RWMutex

	txID    int64
	txTime  time.Time
	txState TxState
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

func (tx *txObj) setState(state TxState) {
	tx.mu.Lock()
	tx.txState = state
	tx.txTime = localtime.Now()
	tx.mu.Unlock()
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
