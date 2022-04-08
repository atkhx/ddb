package storage

type txOpt func(obj *txObj)

func SkipLocked() txOpt {
	return func(obj *txObj) {
		obj.skipLocked = true
	}
}
