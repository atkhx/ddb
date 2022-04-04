package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTxFactory(t *testing.T) {
	factory := NewTxFactory(14)
	assert.Equal(t, int64(14), factory.txCounter)
}

func Test_txFactory_Create(t *testing.T) {
	factory := NewTxFactory(17)

	tx1 := factory.Create()
	tx2 := factory.Create()

	assert.Equal(t, int64(18), tx1.GetID())
	assert.Equal(t, int64(19), tx2.GetID())
}
