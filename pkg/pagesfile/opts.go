package pagesfile

type Option func(f *pagesFile)

func WithPageSize(pageSize uint32) Option {
	return func(f *pagesFile) {
		f.pageSize = pageSize
	}
}

func applyOptions(f *pagesFile, opts ...Option) {
	for _, opt := range opts {
		opt(f)
	}
}
