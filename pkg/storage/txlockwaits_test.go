package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_txLockWaitFactory_Create(t *testing.T) {
	factory := NewTxLockWaitFactory()
	wait := factory.Create()

	assert.NotNil(t, wait)
	assert.Equal(t, 1, cap(wait))
}
