package storage

import (
	"testing"
	"time"

	"github.com/atkhx/ddb/pkg/localtime"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTxAccess_IsWriteable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time { return now }

	type testCase struct {
		txTarget TxObj
		expected bool
	}

	testCases := map[string]testCase{
		"writeable": {
			txTarget: &txObj{txID: 17, txState: TxUncommitted},
			expected: true,
		},
		"not writeable: state committed": {
			txTarget: &txObj{txID: 17, txState: TxCommitted},
			expected: false,
		},
		"not writeable: state rolledback": {
			txTarget: &txObj{txID: 17, txState: TxRolledBack},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, NewTxAccess().IsWriteable(tc.txTarget))
		})
	}
}

func TestTxAccess_IsReadable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	localtime.Now = func() time.Time { return now }

	type testCase struct {
		txInitial TxObj
		txTarget  TxObj
		expected  bool
	}

	testCases := map[string]testCase{
		"not readable: txInitial ID to high": {
			txInitial: &txObj{txID: 18},
			txTarget:  &txObj{txID: 17},
			expected:  false,
		},
		"not readable: equal IDs and rolledback": {
			txInitial: &txObj{txID: 10, txState: TxRolledBack},
			txTarget:  &txObj{txID: 10},
			expected:  false,
		},
		"readable: equal IDs and uncommitted": {
			txInitial: &txObj{txID: 10, txState: TxUncommitted},
			txTarget:  &txObj{txID: 10},
			expected:  true,
		},
		"readable: equal IDs and committed": {
			txInitial: &txObj{txID: 10, txState: TxCommitted},
			txTarget:  &txObj{txID: 10},
			expected:  true,
		},
		"readable: committed before txTarget initialized": {
			txInitial: &txObj{txID: 10, txState: TxCommitted, txTime: now.Add(-time.Hour)},
			txTarget:  &txObj{txID: 11, txTime: now},
			expected:  true,
		},
		"not readable: rollback before txTarget initialized": {
			txInitial: &txObj{txID: 10, txState: TxRolledBack},
			txTarget:  &txObj{txID: 11},
			expected:  false,
		},
		"not readable: not committed": {
			txInitial: &txObj{txID: 10, txState: TxUncommitted},
			txTarget:  &txObj{txID: 11},
			expected:  false,
		},
		"not readable: committed when txTarget initialized": {
			txInitial: &txObj{txID: 10, txState: TxCommitted, txTime: now},
			txTarget:  &txObj{txID: 11, txState: TxUncommitted, txTime: now},
			expected:  false,
		},
		"not readable: committed after txTarget initialized": {
			txInitial: &txObj{txID: 10, txState: TxCommitted, txTime: now.Add(time.Second)},
			txTarget:  &txObj{txID: 11, txState: TxUncommitted, txTime: now},
			expected:  false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, NewTxAccess().IsReadable(tc.txInitial, tc.txTarget))
		})
	}
}
