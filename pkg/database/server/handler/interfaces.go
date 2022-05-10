package handler

import (
	"net"

	"github.com/atkhx/ddb/pkg/database"
)

type Protocol interface {
	CreateConnection(net.Conn) Connection
}

type Connection interface {
	InitConnection() error
	ReadRequest() (database.DbRequest, error)
	WriteResult(database.DbResult) error
}

type Processor interface {
	Execute(database.DbRequest) (database.DbResult, error)
}
