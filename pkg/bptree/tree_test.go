package bptree

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestTree_Get(t *testing.T) {

}

func TestTree_Add(t *testing.T) {

}

func scanRowMussNotBeCalled(t *testing.T) func(row interface{}) bool {
	return func(row interface{}) bool {
		t.Helper()
		t.Fail()
		return false
	}
}

func TestTree_ScanASC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("empty tree", func(t *testing.T) {
		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(&item{
			isLeaf: true,
			isRoot: true,
		}, nil)

		tree := NewTree(3, provider)

		assert.NoError(t, tree.ScanASC(scanRowMussNotBeCalled(t)))
	})

	t.Run("fail on get root tree", func(t *testing.T) {
		expectedErr := errors.New("some error")
		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(nil, expectedErr)

		tree := NewTree(3, provider)

		assert.Equal(t, expectedErr, tree.ScanASC(scanRowMussNotBeCalled(t)))
	})

	t.Run("root is leaf with limit", func(t *testing.T) {
		rows := []interface{}{"row 1", "row 2", "row 3"}
		root := &item{
			isLeaf: true,
			isRoot: true,
			rows:   rows,
		}
		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)

		tree := NewTree(3, provider)

		var callTimer int
		var actualRows []interface{}

		assert.NoError(t, tree.ScanASC(func(row interface{}) bool {
			callTimer++
			actualRows = append(actualRows, row)
			return callTimer == 2
		}))

		assert.Equal(t, []interface{}{"row 1", "row 2"}, actualRows)
	})

	t.Run("root is not leaf without limit", func(t *testing.T) {
		item1 := &item{
			isLeaf:  true,
			itemID:  1,
			rightID: 2,
			rows:    []interface{}{"row 1", "row 2"},
		}
		item2 := &item{
			isLeaf: true,
			itemID: 2,
			rows:   []interface{}{"row 3", "row 4"},
		}

		root := &item{
			isRoot: true,
			iids:   []ItemID{1, 2},
		}

		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)
		provider.EXPECT().LoadItem(1).Return(item1, nil)
		provider.EXPECT().LoadItem(2).Return(item2, nil)

		tree := NewTree(3, provider)

		var actualRows []interface{}

		assert.NoError(t, tree.ScanASC(func(row interface{}) bool {
			actualRows = append(actualRows, row)
			return false
		}))

		assert.Equal(t, []interface{}{"row 1", "row 2", "row 3", "row 4"}, actualRows)
	})

	t.Run("root is not leaf fail to load right", func(t *testing.T) {
		item1 := &item{
			isLeaf:  true,
			itemID:  1,
			rightID: 2,
			rows:    []interface{}{"row 1", "row 2"},
		}

		root := &item{
			isRoot: true,
			iids:   []ItemID{1, 2},
		}

		err := errors.New("some error")

		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)
		provider.EXPECT().LoadItem(1).Return(item1, nil)
		provider.EXPECT().LoadItem(2).Return(nil, err)

		tree := NewTree(3, provider)

		var actualRows []interface{}

		assert.Equal(t, err, tree.ScanASC(func(row interface{}) bool {
			actualRows = append(actualRows, row)
			return false
		}))

		assert.Equal(t, []interface{}{"row 1", "row 2"}, actualRows)
	})

	t.Run("root is not leaf fail to load leaf", func(t *testing.T) {
		root := &item{
			isRoot: true,
			iids:   []ItemID{1, 2},
		}

		err := errors.New("some error")

		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)
		provider.EXPECT().LoadItem(1).Return(nil, err)

		tree := NewTree(3, provider)

		assert.Equal(t, err, tree.ScanASC(scanRowMussNotBeCalled(t)))
	})
}

func TestTree_ScanDESC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("empty tree", func(t *testing.T) {
		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(&item{
			isLeaf: true,
			isRoot: true,
		}, nil)

		tree := NewTree(3, provider)

		assert.NoError(t, tree.ScanDESC(scanRowMussNotBeCalled(t)))
	})

	t.Run("fail on get root tree", func(t *testing.T) {
		expectedErr := errors.New("some error")
		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(nil, expectedErr)

		tree := NewTree(3, provider)

		assert.Equal(t, expectedErr, tree.ScanDESC(scanRowMussNotBeCalled(t)))
	})

	t.Run("root is leaf with limit", func(t *testing.T) {
		rows := []interface{}{"row 1", "row 2", "row 3"}
		root := &item{
			isLeaf: true,
			isRoot: true,
			rows:   rows,
		}
		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)

		tree := NewTree(3, provider)

		var callTimer int
		var actualRows []interface{}

		assert.NoError(t, tree.ScanDESC(func(row interface{}) bool {
			callTimer++
			actualRows = append(actualRows, row)
			return callTimer == 2
		}))

		assert.Equal(t, []interface{}{"row 3", "row 2"}, actualRows)
	})

	t.Run("root is not leaf without limit", func(t *testing.T) {
		item1 := &item{
			isLeaf: true,
			itemID: 1,
			rows:   []interface{}{"row 1", "row 2"},
		}
		item2 := &item{
			isLeaf: true,
			itemID: 2,
			leftID: 1,
			rows:   []interface{}{"row 3", "row 4"},
		}

		root := &item{
			isRoot: true,
			iids:   []ItemID{1, 2},
		}

		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)
		provider.EXPECT().LoadItem(2).Return(item2, nil)
		provider.EXPECT().LoadItem(1).Return(item1, nil)

		tree := NewTree(3, provider)

		var actualRows []interface{}

		assert.NoError(t, tree.ScanDESC(func(row interface{}) bool {
			actualRows = append(actualRows, row)
			return false
		}))

		assert.Equal(t, []interface{}{"row 4", "row 3", "row 2", "row 1"}, actualRows)
	})

	t.Run("root is not leaf fail to load left", func(t *testing.T) {
		item2 := &item{
			isLeaf: true,
			itemID: 2,
			leftID: 1,
			rows:   []interface{}{"row 3", "row 4"},
		}

		root := &item{
			isRoot: true,
			iids:   []ItemID{1, 2},
		}

		err := errors.New("some error")

		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)
		provider.EXPECT().LoadItem(2).Return(item2, nil)
		provider.EXPECT().LoadItem(1).Return(nil, err)

		tree := NewTree(3, provider)

		var actualRows []interface{}

		assert.Equal(t, err, tree.ScanDESC(func(row interface{}) bool {
			actualRows = append(actualRows, row)
			return false
		}))

		assert.Equal(t, []interface{}{"row 4", "row 3"}, actualRows)
	})

	t.Run("root is not leaf fail to load leaf", func(t *testing.T) {
		root := &item{
			isRoot: true,
			iids:   []ItemID{1, 2},
		}

		err := errors.New("some error")

		provider := NewMockItemProvider(ctrl)
		provider.EXPECT().GetRootItem().Return(root, nil)
		provider.EXPECT().LoadItem(2).Return(nil, err)

		tree := NewTree(3, provider)

		assert.Equal(t, err, tree.ScanDESC(scanRowMussNotBeCalled(t)))
	})
}
