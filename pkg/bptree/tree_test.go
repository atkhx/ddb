package bptree

import (
	"testing"

	"github.com/atkhx/ddb/internal/keys"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewTree(t *testing.T) {
	assert.Equal(t, 3, NewTree(3).capacity)
}

func TestTree_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("get on nil root", func(t *testing.T) {
		assert.Nil(t, NewTree(3).Get(keys.IntKey(1)))
	})

	t.Run("get with return row", func(t *testing.T) {
		key := keys.IntKey(1)

		root := NewMockItem(ctrl)
		root.EXPECT().Get(key).Return("some row")

		tree := NewTree(3)
		tree.root = root

		assert.Equal(t, "some row", tree.Get(key))
	})

	t.Run("get with not found row", func(t *testing.T) {
		key := keys.IntKey(1)

		root := NewMockItem(ctrl)
		root.EXPECT().Get(key).Return(nil)

		tree := NewTree(3)
		tree.root = root

		assert.Nil(t, tree.Get(key))
	})
}

func TestTree_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("set on nil root", func(t *testing.T) {
		key := keys.IntKey(1)

		tree := NewTree(3)
		tree.Set(key, "some row")

		assert.Equal(t, "some row", tree.Get(key))
	})

	t.Run("set on not nil root", func(t *testing.T) {
		key := keys.IntKey(1)

		root := NewMockItem(ctrl)
		root.EXPECT().Set(key, "some row")
		root.EXPECT().Split().Return(nil, nil)
		root.EXPECT().Get(key).Return("some row")

		tree := NewTree(3)
		tree.root = root
		tree.Set(key, "some row")

		assert.Equal(t, "some row", tree.Get(key))
	})

	t.Run("set with split", func(t *testing.T) {
		key := keys.IntKey(1)

		splitKey := keys.IntKey(2)
		splitBranch := NewMockItem(ctrl)

		root := NewMockItem(ctrl)
		root.EXPECT().Set(key, "some row")
		root.EXPECT().Split().Return(splitKey, splitBranch)

		tree := NewTree(3)
		tree.root = root
		tree.Set(key, "some row")

		root.EXPECT().Set(key, "row for key 1")
		root.EXPECT().Split().Return(nil, nil)

		splitBranch.EXPECT().Set(splitKey, "row for key 2")
		splitBranch.EXPECT().Split().Return(nil, nil)

		tree.Set(key, "row for key 1")
		tree.Set(splitKey, "row for key 2")
	})
}
