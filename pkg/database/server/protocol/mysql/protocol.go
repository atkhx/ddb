package mysql

import (
	"net"

	"github.com/atkhx/ddb/pkg/database/server/handler"
)

func NewProtocol() *protocol {
	return &protocol{}
}

type protocol struct{}

func (p *protocol) CreateConnection(conn net.Conn) handler.Connection {
	return NewConnection(conn)
}
