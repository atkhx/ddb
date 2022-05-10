package server

var defaults = []Option{
	Host("localhost"),
	Port(3306),
}

func applyOptions(srv *server, options ...Option) {
	for _, option := range options {
		option(srv)
	}
}

type Option func(*server)

func Host(host string) Option {
	return func(s *server) {
		s.host = host
	}
}

func Port(port int) Option {
	return func(s *server) {
		s.port = port
	}
}
