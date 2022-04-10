package keys

import "github.com/atkhx/ddb/internal"

type StrKey string

func (k StrKey) GreaterThan(key internal.Key) bool {
	ikey, ok := key.(StrKey)
	if ok {
		return k > ikey
	}
	panic("incompatible key types")
}

func (k StrKey) String() string {
	return string(k)
}
