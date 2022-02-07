package walog

import (
	"encoding/json"

	"github.com/atkhx/ddb/pkg/localtime"
)

func UnSerialize(data []byte) (rec Record, err error) {
	err = json.Unmarshal(data, &rec)
	return
}

func NewRecord(data []byte) Record {
	return Record{
		Data: data,
		Time: localtime.Now().UnixNano(),
	}
}

type Record struct {
	Data []byte `json:"data"`
	Time int64  `json:"time"`
}

func (r *Record) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
