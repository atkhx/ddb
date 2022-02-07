package lenbytewriter

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBufferedWriter_Write(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLBWriter(ctrl)
	flusher := NewMockFlusher(ctrl)

	content := []byte(`some content`)
	origin.EXPECT().WriteLengthWithData(content).Return(nil)

	assert.NoError(t, NewBufferedWriter(origin, flusher).Write(content))
}

func TestBufferedWriter_Write_ErrorOnWriteData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLBWriter(ctrl)
	flusher := NewMockFlusher(ctrl)

	content := []byte(`some content`)
	origin.EXPECT().WriteLengthWithData(content).Return(errors.New("some error"))

	assert.Error(t, NewBufferedWriter(origin, flusher).Write(content))
}

func TestBufferedWriter_Flush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLBWriter(ctrl)
	flusher := NewMockFlusher(ctrl)
	flusher.EXPECT().Flush().Return(nil)

	assert.NoError(t, NewBufferedWriter(origin, flusher).Flush())
}

func TestBufferedWriter_Flush_ErrorOnFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockLBWriter(ctrl)
	flusher := NewMockFlusher(ctrl)
	flusher.EXPECT().Flush().Return(errors.New("some error"))

	assert.Error(t, NewBufferedWriter(origin, flusher).Flush())
}
