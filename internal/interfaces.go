package internal

type Key interface {
	GreaterThan(Key) bool
}

type Row interface {
}
