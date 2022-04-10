package bptree

import (
	"testing"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/keys"
	"github.com/stretchr/testify/assert"
)

func TestLeaf_Split(t *testing.T) {
	type testCase struct {
		originLeaf     *leaf
		expectedOrigin *leaf

		expectedSplitKey  internal.Key
		expectedSplitLeaf *leaf
	}

	testCases := map[string]testCase{
		"no need split": {
			originLeaf:     NewLeaf(3),
			expectedOrigin: NewLeaf(3),
		},
		"split on capacity 3": {
			originLeaf: &leaf{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
					keys.IntKey(3),
				},
				rows: []internal.Row{
					"row 1",
					"row 2",
					"row 3",
				},
				capacity: 3,
			},
			expectedOrigin: &leaf{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
				},
				rows: []internal.Row{
					"row 1",
					"row 2",
				},
				capacity: 3,
			},
			expectedSplitKey: keys.IntKey(3),
			expectedSplitLeaf: &leaf{
				keys: []internal.Key{
					keys.IntKey(3),
				},
				rows: []internal.Row{
					"row 3",
				},
				capacity: 3,
			},
		},

		"split on capacity 4": {
			originLeaf: &leaf{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
					keys.IntKey(3),
					keys.IntKey(4),
				},
				rows: []internal.Row{
					"row 1",
					"row 2",
					"row 3",
					"row 4",
				},
				capacity: 4,
			},
			expectedOrigin: &leaf{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
				},
				rows: []internal.Row{
					"row 1",
					"row 2",
				},
				capacity: 4,
			},
			expectedSplitKey: keys.IntKey(3),
			expectedSplitLeaf: &leaf{
				keys: []internal.Key{
					keys.IntKey(3),
					keys.IntKey(4),
				},
				rows: []internal.Row{
					"row 3",
					"row 4",
				},
				capacity: 4,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actualKey, actualLeaf := tc.originLeaf.Split()
			if tc.expectedSplitKey == nil && tc.expectedSplitLeaf == nil {
				assert.Nil(t, actualKey)
				assert.Nil(t, actualLeaf)
			} else {
				assert.Equal(t, tc.expectedSplitKey, actualKey)
				assert.Equal(t, tc.expectedSplitLeaf, actualLeaf)
			}

			assert.Equal(t, tc.expectedOrigin, tc.originLeaf)
		})
	}
}
