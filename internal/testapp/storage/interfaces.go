package storage

import internal_storage "github.com/atkhx/ddb/pkg/storage"

type DB interface {
	internal_storage.Storage
}

type TX interface {
	internal_storage.TxObj
}
