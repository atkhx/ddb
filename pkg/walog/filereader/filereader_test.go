package filereader

import (
	"io"
	"testing"

	"github.com/atkhx/ddb/pkg/walog"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestScanner_Scan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	records := []walog.Record{
		walog.NewRecord([]byte(`some content 1`)),
		walog.NewRecord([]byte(`some content 2`)),
	}

	reader := NewMockReader(ctrl)
	reader.EXPECT().Read().Return(records[0], nil)
	reader.EXPECT().Read().Return(records[1], nil)
	reader.EXPECT().Read().Return(walog.Record{}, io.EOF)

	scanIdx := 0
	scanFn := func(record walog.Record) error {
		if assert.Less(t, scanIdx, len(records)) {
			assert.Equal(t, records[scanIdx], record)
			scanIdx++
		}
		return nil
	}

	assert.NoError(t, NewFileReader(NewMockCloser(ctrl), reader).Scan(scanFn))
}

func TestScanner_Scan_ErrorOnScanRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	records := []walog.Record{
		walog.NewRecord([]byte(`some content 1`)),
		walog.NewRecord([]byte(`some content 2`)),
	}

	scanErr := errors.New("some unexpected error")
	reader := NewMockReader(ctrl)
	reader.EXPECT().Read().Return(records[0], nil)
	reader.EXPECT().Read().Return(records[1], nil)
	reader.EXPECT().Read().Return(walog.Record{}, scanErr)

	scanIdx := 0
	scanFn := func(record walog.Record) error {
		if assert.Less(t, scanIdx, len(records)) {
			assert.Equal(t, records[scanIdx], record)
			scanIdx++
		}
		return nil
	}

	err := NewFileReader(NewMockCloser(ctrl), reader).Scan(scanFn)

	assert.Error(t, err)
	assert.Equal(t, scanErr, err)
}

func TestScanner_Scan_ErrorOnCallbackFn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	records := []walog.Record{
		walog.NewRecord([]byte(`some content 1`)),
		walog.NewRecord([]byte(`some content 2`)),
	}

	reader := NewMockReader(ctrl)
	reader.EXPECT().Read().Return(records[0], nil)
	reader.EXPECT().Read().Return(records[1], nil)

	callbackFnErr := errors.New("some callback fn error")
	scanIdx := 0
	scanFn := func(record walog.Record) error {
		if scanIdx > 0 {
			return callbackFnErr
		}

		if assert.Less(t, scanIdx, len(records)) {
			assert.Equal(t, records[scanIdx], record)
			scanIdx++
		}
		return nil
	}

	err := NewFileReader(NewMockCloser(ctrl), reader).Scan(scanFn)

	assert.Error(t, err)
	assert.Equal(t, callbackFnErr, err)
}

func TestScanner_Close(t *testing.T) {
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

			assert.Equal(t, err, NewFileReader(closer, NewMockReader(ctrl)).Close())
		})
	}
}
