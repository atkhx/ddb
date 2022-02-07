package filewriter

import (
	"testing"

	"github.com/atkhx/ddb/pkg/walog"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestLogger_Write(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := map[string]error{
		"without error": nil,
		"with error":    errors.New("file not opened"),
	}

	for name, err := range testCases {
		t.Run(name, func(t *testing.T) {
			record := walog.NewRecord([]byte(`some content`))

			writer := NewMockWriter(ctrl)
			writer.EXPECT().Write(record).Return(0, err)

			assert.Equal(t, err, NewFileWriter(NewMockCloser(ctrl), writer).Write(record))
		})
	}
}

func TestLogger_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := map[string]error{
		"without error": nil,
		"with error":    errors.New("file not opened"),
	}

	for name, err := range testCases {
		t.Run(name, func(t *testing.T) {
			closer := NewMockCloser(ctrl)
			closer.EXPECT().Close().Return(err)

			assert.Equal(t, err, NewFileWriter(closer, NewMockWriter(ctrl)).Close())
		})
	}
}
