package row

import (
	"encoding/json"

	"github.com/atkhx/ddb/pkg/key"
	"github.com/atkhx/ddb/pkg/lsm/storage"
	"github.com/pkg/errors"
)

func NewUnSerializer() *unSerializer {
	return &unSerializer{}
}

type unSerializer struct{}

func (*unSerializer) RowFromBytes(b []byte) (storage.Row, error) {
	return FromBytes(b)
}

type serializableRow struct {
	Key  key.IntKey
	Data interface{}

	RowTxTime int64
	RowDelete bool
}

func FromBytes(b []byte) (*Row, error) {
	var s serializableRow

	if err := json.Unmarshal(b, &s); err != nil {
		return nil, errors.Wrap(err, "unmarshal data failed")
	}

	return &Row{
		key:  s.Key,
		data: s.Data,

		rowTxTime: s.RowTxTime,
		rowDelete: s.RowDelete,
	}, nil
}

func ToBytes(r Row) ([]byte, error) {
	return json.Marshal(serializableRow{
		Key:  r.key,
		Data: r.data,

		RowTxTime: r.rowTxTime,
		RowDelete: r.rowDelete,
	})
}
