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
	assert.Equal(t, readCommitted, tx.GetIsolation())
}

func TestTxObj_IsReadable(t *testing.T) {
	type testCase struct {
		txObj    TxObj
		expected bool
	}

	txObjCommitted := NewTxObj(2)
	txObjCommitted.commit()

	txObjRolledBack := NewTxObj(3)
	txObjRolledBack.rollback()

	testCases := map[string]testCase{
		"uncommitted": {
			txObj:    NewTxObj(1),
			expected: true,
		},
		"committed": {
			txObj:    txObjCommitted,
			expected: true,
		},
		"rolledBack": {
			txObj:    txObjRolledBack,
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.txObj.IsReadable())
		})
	}
}

func TestTxObj_IsWriteable(t *testing.T) {
	type testCase struct {
		txObj    TxObj
		expected bool
	}

	txObjCommitted := NewTxObj(2)
	txObjCommitted.commit()

	txObjRolledBack := NewTxObj(3)
	txObjRolledBack.rollback()

	testCases := map[string]testCase{
		"uncommitted": {
			txObj:    NewTxObj(1),
			expected: true,
		},
		"committed": {
			txObj:    txObjCommitted,
			expected: false,
		},
		"rolledBack": {
			txObj:    txObjRolledBack,
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.txObj.IsWriteable())
		})
	}
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
