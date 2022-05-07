package storage

import (
	"sync/atomic"
)

func NewTxFactory(txCounter int64) *txFactory {
	return &txFactory{txCounter: txCounter}
}

type txFactory struct {
	txCounter int64
}

func (f *txFactory) Create(options ...TxOpt) TxObj {
	return NewTxObj(atomic.AddInt64(&f.txCounter, 1), options...)
}
