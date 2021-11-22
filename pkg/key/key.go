// Package key provides base Key interface for data structure algorithms
// and implementations for simple data type keys
package key

type Key interface {
	Equal(Key) bool
	Less(Key) bool
	CompareWith(Key) CompareResult
}
