package writer

import (
	"testing"

	"github.com/atkhx/ddb/pkg/walog"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestWriter_Write(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLenByteWriter(ctrl)

	record := walog.NewRecord([]byte("some content"))
	content, err := record.Serialize()

	assert.NoError(t, err)

	origin.EXPECT().Write(content).Return(nil)
	origin.EXPECT().Flush().Return(nil)

	n, err := NewWriter(origin).Write(record)

	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
}

func TestWriter_Write_ErrorOnFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLenByteWriter(ctrl)

	record := walog.NewRecord([]byte("some content"))
	content, err := record.Serialize()

	assert.NoError(t, err)

	origin.EXPECT().Write(content).Return(nil)
	origin.EXPECT().Flush().Return(errors.New("some error"))

	_, err = NewWriter(origin).Write(record)
	assert.Error(t, err)
}

func TestWriter_Write_ErrorOnWriteData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLenByteWriter(ctrl)

	record := walog.NewRecord([]byte("some content"))
	content, err := record.Serialize()

	assert.NoError(t, err)

	origin.EXPECT().Write(content).Return(errors.New("some error"))

	_, err = NewWriter(origin).Write(record)
	assert.Error(t, err)
}
