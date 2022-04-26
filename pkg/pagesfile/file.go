package pagesfile

import (
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrReadPageBytesLessThanNeed  = errors.New("read Page data less than need")
	ErrWritePageBytesLessThanNeed = errors.New("write Page data less than need")

	defaultOptions = []Option{
		WithPageSize(4 * 1024),
	}
)

func Open(filename string, customOptions ...Option) (*pagesFile, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	res := &pagesFile{file: f}

	applyOptions(res, defaultOptions...)
	applyOptions(res, customOptions...)

	i, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if res.pageSize < pageMinSize {
		return nil, ErrToSmallPageSize
	}

	res.counter = uint32(i.Size() / int64(res.pageSize))
	return res, nil
}

func Create(filename string, customOptions ...Option) (*pagesFile, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	res := &pagesFile{file: f}

	applyOptions(res, defaultOptions...)
	applyOptions(res, customOptions...)

	if res.pageSize < pageMinSize {
		return nil, ErrToSmallPageSize
	}

	return res, nil
}

type pagesFile struct {
	pageSize uint32
	counter  uint32

	file *os.File
}

func (f *pagesFile) GetNextIndex() uint32 {
	return atomic.AddUint32(&f.counter, 1)
}

func (f *pagesFile) getOffset(index uint32) int64 {
	return int64(index * f.pageSize)
}

func (f *pagesFile) Read(index uint32) (*Page, error) {
	buf := make([]byte, f.pageSize)

	n, err := f.file.ReadAt(buf, f.getOffset(index))
	if err != nil {
		return nil, err
	}

	if n != int(f.pageSize) {
		return nil, ErrReadPageBytesLessThanNeed
	}

	return UnSerialize(index, buf)
}

func (f *pagesFile) Write(current *Page) error {
	b, err := current.Serialize()
	if err != nil {
		return err
	}

	n, err := f.file.WriteAt(b, f.getOffset(current.index))
	if err != nil {
		return err
	}

	if n != int(f.pageSize) {
		return ErrWritePageBytesLessThanNeed
	}
	return nil
}

func (f *pagesFile) Load(index uint32) (*Page, error) {
	result, err := f.Read(index)
	if err != nil {
		return nil, err
	}

	var current = result
	for current.next != 0 {
		next, err := f.Read(current.next)
		if err != nil {
			return nil, err
		}

		current.nextPage = next
		current = next
	}

	return result, nil
}

func (f *pagesFile) NewPage(index uint32) *Page {
	return &Page{
		index:      index,
		wasCreated: true,
		data:       make([]byte, f.pageSize),
	}
}

func (f *pagesFile) Save(current *Page) error {
	var previous *Page

	for current != nil {
		if current.wasCreated {
			current.wasCreated = false
			current.index = f.GetNextIndex()

			if previous != nil {
				previous.next = current.index
			}
		}

		previous = current
		current = current.nextPage
	}

	current = current
	for current != nil {
		if err := f.Write(current); err != nil {
			return err
		}

		current = current.nextPage
	}
	return nil
}

func (f *pagesFile) Close() error {
	return f.file.Close()
}
