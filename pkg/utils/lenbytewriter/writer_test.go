package lenbytewriter

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

func TestWriter_WriteLengthWithData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockWriter(ctrl)

	contentText := `some content`
	content := makeRowBytesWithLength(t, contentText)

	origin.EXPECT().Write(content[:4]).Return(4, nil)
	origin.EXPECT().Write(content[4:]).Return(len(content[4:]), nil)

	assert.NoError(t, NewWriter(origin).WriteLengthWithData([]byte(contentText)))
}

func TestWriter_WriteLengthWithData_ErrorOnWriteData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockWriter(ctrl)

	contentText := `some content`
	content := makeRowBytesWithLength(t, contentText)

	err := errors.New("some error")

	origin.EXPECT().Write(content[:4]).Return(4, nil)
	origin.EXPECT().Write(content[4:]).Return(0, err)

	assert.Error(t, NewWriter(origin).WriteLengthWithData([]byte(contentText)))
}

func TestWriter_WriteLengthWithData_ErrorOnWriteLen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	origin := NewMockWriter(ctrl)

	contentText := `some content`
	content := makeRowBytesWithLength(t, contentText)

	err := errors.New("some error")

	origin.EXPECT().Write(content[:4]).Return(0, err)
	assert.Error(t, NewWriter(origin).WriteLengthWithData([]byte(contentText)))
}
