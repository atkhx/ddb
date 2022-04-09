package storage

type txOpt func(obj *txObj)

func ReadCommitted() txOpt {
	return func(obj *txObj) {
		obj.skipLocked = false
		obj.txIsolation = readCommitted
	}
}

func RepeatableRead() txOpt {
	return func(obj *txObj) {
		obj.skipLocked = true
		obj.txIsolation = repeatableRead
	}
}
