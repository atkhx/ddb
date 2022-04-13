package bptree

import (
	"testing"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/keys"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestBranch_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leftItem := NewMockItem(ctrl)
	rightItem := NewMockItem(ctrl)

	branch := &branch{
		keys:  []internal.Key{keys.IntKey(2)},
		items: []Item{leftItem, rightItem},
	}

	t.Run("get from left item", func(t *testing.T) {
		key := keys.IntKey(1)
		leftItem.EXPECT().Get(key).Return("some row")
		assert.Equal(t, "some row", branch.Get(key))
	})

	t.Run("get from right item by equal split key", func(t *testing.T) {
		key := keys.IntKey(2)
		rightItem.EXPECT().Get(key).Return("some row")
		assert.Equal(t, "some row", branch.Get(key))
	})

	t.Run("get from right item by greater split key", func(t *testing.T) {
		key := keys.IntKey(3)
		rightItem.EXPECT().Get(key).Return("some row")
		assert.Equal(t, "some row", branch.Get(key))
	})
}

func TestBranch_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leftItem := NewMockItem(ctrl)
	rightItem := NewMockItem(ctrl)

	branch := &branch{
		keys:     []internal.Key{keys.IntKey(2)},
		items:    []Item{leftItem, rightItem},
		capacity: 0,
	}

	t.Run("insert left", func(t *testing.T) {
		leftItem.EXPECT().Set(keys.IntKey(1), "row 1")
		leftItem.EXPECT().Split().Return(nil, nil)

		branch.Set(keys.IntKey(1), "row 1")
	})

	t.Run("insert right by equal key", func(t *testing.T) {
		rightItem.EXPECT().Set(keys.IntKey(2), "row 2")
		rightItem.EXPECT().Split().Return(nil, nil)

		branch.Set(keys.IntKey(2), "row 2")
	})

	t.Run("insert right by greater key", func(t *testing.T) {
		rightItem.EXPECT().Set(keys.IntKey(3), "row 3")
		rightItem.EXPECT().Split().Return(nil, nil)

		branch.Set(keys.IntKey(3), "row 3")
	})
}

func TestBranch_SetWithSplitLeft(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leftItem := NewMockItem(ctrl)
	rightItem := &leaf{keys: []internal.Key{keys.IntKey(3)}}

	originBranch := &branch{
		keys:  []internal.Key{keys.IntKey(2)},
		items: []Item{leftItem, rightItem},
	}

	splitKey := keys.IntKey(0)
	splitItem := &leaf{keys: []internal.Key{keys.IntKey(1)}}

	leftItem.EXPECT().Set(keys.IntKey(1), "row 1")
	leftItem.EXPECT().Split().Return(splitKey, splitItem)

	originBranch.Set(keys.IntKey(1), "row 1")

	assert.Equal(t, []internal.Key{
		keys.IntKey(0),
		keys.IntKey(2),
	}, originBranch.keys)

	assert.Equal(t, []Item{
		leftItem,
		splitItem,
		rightItem,
	}, originBranch.items)
}

func TestBranch_SetWithSplitRight(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leftItem := &leaf{keys: []internal.Key{keys.IntKey(1)}}
	rightItem := NewMockItem(ctrl)

	originBranch := &branch{
		keys:  []internal.Key{keys.IntKey(2)},
		items: []Item{leftItem, rightItem},
	}

	splitKey := keys.IntKey(3)
	splitItem := &leaf{keys: []internal.Key{keys.IntKey(1)}}

	rightItem.EXPECT().Set(keys.IntKey(2), "row 2")
	rightItem.EXPECT().Split().Return(splitKey, splitItem)

	originBranch.Set(keys.IntKey(2), "row 2")

	assert.Equal(t, []internal.Key{
		keys.IntKey(2),
		keys.IntKey(3),
	}, originBranch.keys)

	assert.Equal(t, []Item{
		leftItem,
		rightItem,
		splitItem,
	}, originBranch.items)
}

func TestBranch_SetWithSplitMiddle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leftItem := &leaf{keys: []internal.Key{keys.IntKey(1)}}
	middleItem := NewMockItem(ctrl)
	rightItem := &leaf{keys: []internal.Key{keys.IntKey(3)}}

	originBranch := &branch{
		keys:  []internal.Key{keys.IntKey(1), keys.IntKey(3)},
		items: []Item{leftItem, middleItem, rightItem},
	}

	splitKey := keys.IntKey(2)
	splitItem := &leaf{keys: []internal.Key{keys.IntKey(2)}}

	middleItem.EXPECT().Set(keys.IntKey(2), "row 2")
	middleItem.EXPECT().Split().Return(splitKey, splitItem)

	originBranch.Set(keys.IntKey(2), "row 2")

	assert.Equal(t, []internal.Key{
		keys.IntKey(1),
		keys.IntKey(2),
		keys.IntKey(3),
	}, originBranch.keys)

	assert.Equal(t, []Item{
		leftItem,
		middleItem,
		splitItem,
		rightItem,
	}, originBranch.items)
}

func TestBranch_Split(t *testing.T) {
	type testCase struct {
		originBranch   *branch
		expectedOrigin *branch

		expectedSplitKey  internal.Key
		expectedSplitItem Item
	}

	testCases := map[string]testCase{
		"no need split": {
			originBranch:   NewBranch(3),
			expectedOrigin: NewBranch(3),
		},
		"split on capacity 3": {
			originBranch: &branch{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
					keys.IntKey(3),
				},
				items: []Item{
					&leaf{keys: []internal.Key{keys.IntKey(1)}, rows: nil, capacity: 3},
					&leaf{keys: []internal.Key{keys.IntKey(2)}, rows: nil, capacity: 3},
					&leaf{keys: []internal.Key{keys.IntKey(3)}, rows: nil, capacity: 3},
					&leaf{keys: []internal.Key{keys.IntKey(4)}, rows: nil, capacity: 3},
				},
				capacity: 3,
			},
			expectedOrigin: &branch{
				keys: []internal.Key{
					keys.IntKey(1),
				},
				items: []Item{
					&leaf{keys: []internal.Key{keys.IntKey(1)}, rows: nil, capacity: 3},
					&leaf{keys: []internal.Key{keys.IntKey(2)}, rows: nil, capacity: 3},
				},
				capacity: 3,
			},
			expectedSplitKey: keys.IntKey(2),
			expectedSplitItem: &branch{
				keys: []internal.Key{
					keys.IntKey(3),
				},
				items: []Item{
					&leaf{keys: []internal.Key{keys.IntKey(3)}, rows: nil, capacity: 3},
					&leaf{keys: []internal.Key{keys.IntKey(4)}, rows: nil, capacity: 3},
				},
				capacity: 3,
			},
		},
		"split on capacity 4": {
			originBranch: &branch{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
					keys.IntKey(3),
					keys.IntKey(4),
				},
				items: []Item{
					&leaf{keys: []internal.Key{keys.IntKey(1)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(2)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(3)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(4)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(5)}, rows: nil, capacity: 4},
				},
				capacity: 4,
			},
			expectedOrigin: &branch{
				keys: []internal.Key{
					keys.IntKey(1),
					keys.IntKey(2),
				},
				items: []Item{
					&leaf{keys: []internal.Key{keys.IntKey(1)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(2)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(3)}, rows: nil, capacity: 4},
				},
				capacity: 4,
			},
			expectedSplitKey: keys.IntKey(3),
			expectedSplitItem: &branch{
				keys: []internal.Key{
					keys.IntKey(4),
				},
				items: []Item{
					&leaf{keys: []internal.Key{keys.IntKey(4)}, rows: nil, capacity: 4},
					&leaf{keys: []internal.Key{keys.IntKey(5)}, rows: nil, capacity: 4},
				},
				capacity: 4,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actualKey, actualItem := tc.originBranch.Split()
			if tc.expectedSplitKey == nil && tc.expectedSplitItem == nil {
				assert.Nil(t, actualKey)
				assert.Nil(t, actualItem)
			} else {
				assert.Equal(t, tc.expectedSplitKey, actualKey)
				assert.Equal(t, tc.expectedSplitItem, actualItem)
			}

			assert.Equal(t, tc.expectedOrigin, tc.originBranch)
		})
	}
}
