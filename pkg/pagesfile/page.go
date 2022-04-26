package pagesfile

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

var ErrToSmallPageSize = errors.New("to small Page size")

// pageMetaSize - количество зарезервированных байт под next & size
const pageMetaSize = 4 * 2

// pageMinSize - минимальный размер станицы (16 - просто минимальная порция данных)
const pageMinSize = pageMetaSize + 16

// Page - Единица разметки файла. Для хранения данных могут использоваться несколько страниц.
// В таком случае, поле next указывает на индекс следующей страницы в файле
type Page struct {
	used uint32 // Сколько байт реально используется
	next uint32 // Индекс следующей страницы (0 == NULL)
	data []byte // Данные страницы в исходном виде

	index uint32

	wasCreated bool
	wasChanged bool

	nextPage *Page
}

func (p *Page) ReadContent() []byte {
	var result []byte
	for item := p; item != nil; item = item.nextPage {
		copy(result, item.data[pageMetaSize:p.used])
	}
	return result
}

func (p *Page) WriteContent(data []byte) {
	contentSize := len(p.data) - pageMetaSize
	contentChunks := len(data) / contentSize

	if len(data)%contentSize > 0 {
		contentChunks++
	}

	item := p

	for i := 0; i < contentChunks; i++ {
		copySize := contentSize
		if copySize > len(data[i*contentSize:i*contentSize+copySize]) {
			copySize = len(data) - i*contentSize
		}

		copy(item.data[pageMetaSize:copySize], data[i*contentSize:i*contentSize+copySize])
		item.used = uint32(copySize)

		if i < contentChunks-1 && item.nextPage == nil {
		}
	}

}

func (p *Page) Serialize() ([]byte, error) {
	writer := bytes.NewBuffer(nil)

	if err := binary.Write(writer, binary.BigEndian, p.used); err != nil {
		return nil, err
	}

	if err := binary.Write(writer, binary.BigEndian, p.next); err != nil {
		return nil, err
	}

	if err := binary.Write(writer, binary.BigEndian, p.data[pageMetaSize:]); err != nil {
		return nil, err
	}

	return writer.Bytes(), nil
}

func UnSerialize(index uint32, origin []byte) (*Page, error) {
	reader := bytes.NewBuffer(origin)
	result := &Page{index: index}

	if err := binary.Read(reader, binary.BigEndian, &result.used); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.BigEndian, &result.next); err != nil {
		return nil, err
	}

	result.data = make([]byte, len(origin))
	copy(result.data, origin)

	return result, nil
}
