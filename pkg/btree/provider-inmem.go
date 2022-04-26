package btree

func NewInmemProvider() *inmemProvider {
	result := &inmemProvider{}
	result.root = result.GetNewLeaf()

	return result
}

type inmemProvider struct {
	root *item
}

func (p *inmemProvider) GetRootItem() *item {
	return p.root
}

func (p *inmemProvider) LoadItem(itemID ItemID) *item {
	if itemID == nil {
		return p.root
	}

	item, ok := itemID.(*item)
	if !ok {
		panic("invalid itemID")
	}

	return item
}

func (p *inmemProvider) SaveItem(item *item) {
	if item.parentID == nil {
		p.root = item
	}
}

func (p *inmemProvider) GetNewBranch() *item {
	result := &item{isLeaf: false}
	result.itemID = result

	return result
}

func (p *inmemProvider) GetNewLeaf() *item {
	result := &item{isLeaf: true}
	result.itemID = result

	return result
}
