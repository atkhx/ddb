package storage

func NewTxLockWaitFactory() *txLockWaitFactory {
	return &txLockWaitFactory{}
}

type txLockWaitFactory struct {
}

func (f *txLockWaitFactory) Create() waitChan {
	return make(waitChan, 1)
}
