package btree_single

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/atkhx/ddb/pkg/pagesfile"
)

type PagesStorage interface {
	GetNextIndex() uint32
	NewPage() *pagesfile.Page
	Load(index uint32) (*pagesfile.Page, error)
	Save(current *pagesfile.Page) error
}

type fileProvider struct {
	storage  PagesStorage
	newItems map[uint32]bool
}

type RootInfo struct {
	RootID uint32
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (p *fileProvider) GetRootItem() *item {
	rootPage, err := p.storage.Load(0)
	panicOnErr(err)

	rootInfo := RootInfo{}
	panicOnErr(json.Unmarshal(rootPage.ReadContent(), &rootInfo))

	return p.LoadItem(rootInfo.RootID)
}

func (p *fileProvider) LoadItem(itemID ItemID) *item {
	itemPage, err := p.storage.Load(itemID.(uint32))
	panicOnErr(err)

	buf := bytes.NewBuffer(itemPage.ReadContent())
	res := &item{}

	var parentId, rightId uint32

	panicOnErr(binary.Read(buf, binary.BigEndian, &res.isLeaf))
	panicOnErr(binary.Read(buf, binary.BigEndian, &parentId))
	panicOnErr(binary.Read(buf, binary.BigEndian, &rightId))
	panicOnErr(binary.Read(buf, binary.BigEndian, &res.keys))

	res.itemID = itemID
	res.parentID = parentId
	res.rightID = rightId

	if res.isLeaf {
		panicOnErr(binary.Read(buf, binary.BigEndian, &res.rows))
	} else {
		panicOnErr(binary.Read(buf, binary.BigEndian, &res.iids))
	}

	return res
}

func (p *fileProvider) SaveItem(item *item) {
	var itemPage *pagesfile.Page
	var err error

	if p.newItems[item.itemID.(uint32)] {
		defer func() {
			delete(p.newItems, item.itemID.(uint32))
		}()

		itemPage = p.storage.NewPage()
	} else {
		itemPage, err = p.storage.Load(item.itemID.(uint32))
		panicOnErr(err)
	}

	buf := bytes.NewBuffer(itemPage.ReadContent())

	panicOnErr(binary.Write(buf, binary.BigEndian, item.isLeaf))
	panicOnErr(binary.Read(buf, binary.BigEndian, item.parentID.(uint32)))
	panicOnErr(binary.Read(buf, binary.BigEndian, item.rightID.(uint32)))
	panicOnErr(binary.Read(buf, binary.BigEndian, item.keys))

	if item.isLeaf {
		panicOnErr(binary.Read(buf, binary.BigEndian, item.rows))
	} else {
		panicOnErr(binary.Read(buf, binary.BigEndian, item.iids))
	}

	itemPage.WriteContent(buf.Bytes())
	panicOnErr(p.storage.Save(itemPage))
}

func (p *fileProvider) GetNewBranch() *item {
	itemId := p.storage.GetNextIndex()
	p.newItems[itemId] = true

	return &item{
		isLeaf: false,
		itemID: itemId,
	}
}

func (p *fileProvider) GetNewLeaf() *item {
	itemId := p.storage.GetNextIndex()
	p.newItems[itemId] = true
	return &item{
		isLeaf: true,
		itemID: itemId,
	}
}
