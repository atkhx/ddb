package lenbytereader

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func makeRowBytesWithLength(t *testing.T, textContent string) []byte {
	buf := bytes.NewBuffer([]byte{})
	if err := binary.Write(buf, binary.BigEndian, uint32(len(textContent))); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(buf, binary.BigEndian, []byte(textContent)); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func TestReader_Read(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originReader := NewMockReader(ctrl)

	textContent := `some text`
	rowBytes := makeRowBytesWithLength(t, textContent)

	// read length
	originReader.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
		copy(b, rowBytes[:4])
		return 4, nil
	})

	// read row
	originReader.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
		copy(b, rowBytes[4:])
		return len(rowBytes[4:]), nil
	})

	r, err := NewReader(originReader).Read()
	assert.NoError(t, err)
	assert.Equal(t, []byte(textContent), r)
}

func TestReader_Read_ErrorOnReadLength(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originReader := NewMockReader(ctrl)

	readError := errors.New("some error")
	originReader.EXPECT().Read(gomock.Any()).Return(0, readError)

	_, err := NewReader(originReader).Read()
	assert.Error(t, err)
	assert.Equal(t, "read len failed: some error", err.Error())
}

func TestReader_Read_ErrorOnReadRow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originReader := NewMockReader(ctrl)

	textContent := `some text`
	rowBytes := makeRowBytesWithLength(t, textContent)

	// first part of length bytes
	originReader.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
		copy(b, rowBytes[:4])
		return 4, nil
	})

	readError := errors.New("some error")
	originReader.EXPECT().Read(gomock.Any()).Return(0, readError)

	_, err := NewReader(originReader).Read()
	assert.Error(t, err)
	assert.Equal(t, "read bytes failed: some error", err.Error())
}
