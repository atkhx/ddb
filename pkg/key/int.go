package key

type IntKey int

func (k IntKey) Equal(key Key) bool {
	return k.CompareWith(key) == CompareResultEqual
}

func (k IntKey) Less(key Key) bool {
	return k.CompareWith(key) == CompareResultLess
}

func (k IntKey) CompareWith(key Key) CompareResult {
	if k == key.(IntKey) {
		return CompareResultEqual
	}
	if k < key.(IntKey) {
		return CompareResultLess
	}

	return CompareResultGreater
}
