package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesKey_CompareWith(t *testing.T) {
	key := BytesKey("banana")

	assert.True(t, key.CompareWith(BytesKey("banana1")).IsLess())
	assert.True(t, key.CompareWith(BytesKey("banana")).IsEqual())
	assert.True(t, key.CompareWith(BytesKey("banan")).IsGreater())
}

func TestIntKey_CompareWith(t *testing.T) {
	key := IntKey(18)

	assert.True(t, key.CompareWith(IntKey(20)).IsLess())
	assert.True(t, key.CompareWith(IntKey(18)).IsEqual())
	assert.True(t, key.CompareWith(IntKey(11)).IsGreater())
}

func TestStrKey_CompareWith(t *testing.T) {
	key := StrKey("banana")

	assert.True(t, key.CompareWith(StrKey("banana1")).IsLess())
	assert.True(t, key.CompareWith(StrKey("banana")).IsEqual())
	assert.True(t, key.CompareWith(StrKey("banan")).IsGreater())
}
