package reader

import (
	"testing"

	"github.com/atkhx/ddb/pkg/localtime"
	"github.com/atkhx/ddb/pkg/walog"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReader_Read(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	localtime.SetCurrentTime(t, "2022-03-28 10:00:00")

	readData := []byte(`{"data": "c29tZSBkYXRh", "time": 1648461600000000000}`)

	origin := NewMockLenByteReader(ctrl)
	origin.EXPECT().Read().Return(readData, nil)

	r, err := NewReader(origin).Read()
	assert.NoError(t, err)
	assert.Equal(t, walog.NewRecord([]byte("some data")), r)
}

func TestReader_Read_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readError := errors.New("some error")

	originReader := NewMockLenByteReader(ctrl)
	originReader.EXPECT().Read().Return(nil, readError)

	_, err := NewReader(originReader).Read()
	assert.Error(t, err)
	assert.Equal(t, "read data failed: some error", err.Error())
}
