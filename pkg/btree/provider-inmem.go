package btree

import "github.com/pkg/errors"

var (
	ErrInvalidItemID = errors.New("invalid itemID")
)

func NewInmemProvider() *inmemProvider {
	result := &inmemProvider{}

	r, err := result.GetNewLeaf()
	if err != nil {
		panic(err)
	}

	result.root = r
	result.root.isRoot = true

	return result
}

type inmemProvider struct {
	root *item
}

func (p *inmemProvider) GetRootItem() (*item, error) {
	return p.root, nil
}

func (p *inmemProvider) LoadItem(itemID ItemID) (*item, error) {
	if itemID == nil {
		return p.root, nil
	}

	item, ok := itemID.(*item)
	if !ok {
		return nil, ErrInvalidItemID
	}

	return item, nil
}

func (p *inmemProvider) SaveItem(item *item) error {
	if item.isRoot {
		p.root = item
	}
	return nil
}

func (p *inmemProvider) GetNewBranch() (*item, error) {
	result := &item{isLeaf: false}
	result.itemID = result

	return result, nil
}

func (p *inmemProvider) GetNewLeaf() (*item, error) {
	result := &item{isLeaf: true}
	result.itemID = result

	return result, nil
}
