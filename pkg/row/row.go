package row

import (
	"time"

	"github.com/atkhx/ddb/pkg/base"
)

func New(key base.IntKey, data interface{}) *Row {
	return &Row{
		key:  key,
		data: data,
	}
}

type Row struct {
	key  base.IntKey
	data interface{}

	rowTxTime int64
	rowDelete bool
}

func (r *Row) Key() base.Key {
	return r.key
}

func (r *Row) IsDeleted() bool {
	return r.rowDelete
}

func (r *Row) Data() interface{} {
	return r.data
}

func (r *Row) MakeDeleted() {
	r.rowDelete = true
	r.rowTxTime = time.Now().UnixNano()
}

func (r *Row) Serialize() ([]byte, error) {
	return ToBytes(*r)
}
