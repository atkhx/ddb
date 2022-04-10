package keys

import "github.com/atkhx/ddb/internal"

type IntKey int

func (k IntKey) GreaterThan(key internal.Key) bool {
	ikey, ok := key.(IntKey)
	if ok {
		return k > ikey
	}
	panic("incompatible key types")
}
