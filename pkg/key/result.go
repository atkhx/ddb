package key

type CompareResult int

const (
	CompareResultLess    = CompareResult(-1)
	CompareResultEqual   = CompareResult(0)
	CompareResultGreater = CompareResult(1)
)

func (r CompareResult) IsLess() bool {
	return r == CompareResultLess
}

func (r CompareResult) IsEqual() bool {
	return r == CompareResultEqual
}

func (r CompareResult) IsGreater() bool {
	return r == CompareResultGreater
}
