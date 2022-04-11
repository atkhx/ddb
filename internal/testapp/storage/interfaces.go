package storage

import internal_storage "github.com/atkhx/ddb/internal/storage"

type DB interface {
	internal_storage.Storage
}

type TX interface {
	internal_storage.TxObj
}
