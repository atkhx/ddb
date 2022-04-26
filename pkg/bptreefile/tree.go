package bptreefile

import "github.com/atkhx/ddb/pkg/pagesfile"

type PagesFile interface {
	Load(index uint32) (*pagesfile.page, error)
	Save(*pagesfile.page) error
}

type treeFile struct {
	pagesFile PagesFile
}
