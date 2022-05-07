package storage

type TxOpt func(obj *txObj)

func ReadCommitted() TxOpt {
	return func(obj *txObj) {
		obj.txIsolation = readCommitted
	}
}

func RepeatableRead() TxOpt {
	return func(obj *txObj) {
		obj.txIsolation = repeatableRead
	}
}
