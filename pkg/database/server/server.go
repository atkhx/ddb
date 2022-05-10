package server

import (
	"context"
	"fmt"
	"net"
	"sync"
)

func NewServer(handler Handler, options ...Option) *server {
	srv := &server{}
	srv.handler = handler

	applyOptions(srv, defaults...)
	applyOptions(srv, options...)

	return srv
}

type server struct {
	host string
	port int

	handler Handler
}

func (s *server) getAddr() string {
	return fmt.Sprintf("%s:%d", s.host, s.port)
}

func (s *server) Listen(ctx context.Context) (err error) {
	l, err := net.Listen("tcp", s.getAddr())
	if err != nil {
		return err
	}
	defer func() {
		_ = l.Close()
	}()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for !s.contextDone(ctx) {
		c, e := l.Accept()
		if e != nil {
			err = e
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handler.Handle(c)
			fmt.Println("close connection")
		}()
	}
	return
}

func (s *server) contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
