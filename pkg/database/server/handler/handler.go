package handler

import (
	"fmt"
	"io"
	"net"
)

func NewHandler(protocol Protocol, processor Processor) *handler {
	return &handler{protocol, processor}
}

type handler struct {
	protocol  Protocol
	processor Processor
}

func (h *handler) Handle(conn net.Conn) {
	var err error
	defer func() {
		if err != nil {
			fmt.Println("conn closed by error", err)
		}
	}()

	pc := h.protocol.CreateConnection(conn)
	if err = pc.InitConnection(); err != nil {
		return
	}

	for {
		dbRequest, e := pc.ReadRequest()
		if e != nil && e == io.EOF {
			return
		}

		if e != nil {
			err = e
			return
		}

		dbResult, e := h.processor.Execute(dbRequest)
		if e != nil {
			err = e
			return
		}

		if e = pc.WriteResult(dbResult); e != nil {
			err = e
			return
		}
	}
}
