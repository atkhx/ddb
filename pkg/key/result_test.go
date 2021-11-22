package key

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareResult_IsEqual(t *testing.T) {
	assert.True(t, CompareResultEqual.IsEqual())
	assert.False(t, CompareResultLess.IsEqual())
	assert.False(t, CompareResultGreater.IsEqual())
}

func TestCompareResult_IsGreater(t *testing.T) {
	assert.False(t, CompareResultEqual.IsGreater())
	assert.False(t, CompareResultLess.IsGreater())
	assert.True(t, CompareResultGreater.IsGreater())
}

func TestCompareResult_IsLess(t *testing.T) {
	assert.False(t, CompareResultEqual.IsLess())
	assert.True(t, CompareResultLess.IsLess())
	assert.False(t, CompareResultGreater.IsLess())

}
