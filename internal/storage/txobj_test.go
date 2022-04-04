package storage

import (
	"testing"
	"time"

	"github.com/atkhx/ddb/pkg/localtime"
	"github.com/stretchr/testify/assert"
)

func TestNewTxObj(t *testing.T) {
	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	txID := int64(14)
	tx := NewTxObj(txID)

	assert.Equal(t, txID, tx.GetID())
	assert.Equal(t, TxUncommitted, tx.GetState())
	assert.Equal(t, now, tx.GetTime())
}

func TestTxObj_commit(t *testing.T) {
	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	txID := int64(14)
	tx := NewTxObj(txID)

	now.Add(time.Second)
	tx.commit()

	assert.Equal(t, txID, tx.GetID())
	assert.Equal(t, TxCommitted, tx.GetState())
	assert.Equal(t, now, tx.GetTime())
}

func TestTxObj_rollback(t *testing.T) {
	now := time.Now()
	localtime.Now = func() time.Time {
		return now
	}

	txID := int64(14)
	tx := NewTxObj(txID)

	now.Add(time.Second)
	tx.rollback()

	assert.Equal(t, txID, tx.GetID())
	assert.Equal(t, TxRolledBack, tx.GetState())
	assert.Equal(t, now, tx.GetTime())
}
