package key

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntKey_CompareWith(t *testing.T) {
	key := IntKey(18)

	assert.Equal(t, CompareResultLess, key.CompareWith(IntKey(20)))
	assert.Equal(t, CompareResultEqual, key.CompareWith(IntKey(18)))
	assert.Equal(t, CompareResultGreater, key.CompareWith(IntKey(11)))
}

func TestIntKey_Equal(t *testing.T) {
	assert.True(t, IntKey(11).Equal(IntKey(11)))
}

func TestIntKey_Less(t *testing.T) {
	assert.True(t, IntKey(17).Less(IntKey(21)))
}
