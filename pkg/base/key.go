package base

import "bytes"

type Key interface {
	CompareWith(Key) CompareResult
}

type BytesKey []byte

func (k BytesKey) CompareWith(key Key) CompareResult {
	return CompareResult(bytes.Compare(k, key.(BytesKey)))
}

type IntKey int

func (k IntKey) CompareWith(key Key) CompareResult {
	if k == key.(IntKey) {
		return CompareResultEqual
	}
	if k < key.(IntKey) {
		return CompareResultLess
	}

	return CompareResultGreater
}

type StrKey string

func (k StrKey) CompareWith(key Key) CompareResult {
	if k == key.(StrKey) {
		return CompareResultEqual
	}
	if k < key.(StrKey) {
		return CompareResultLess
	}

	return CompareResultGreater
}
