package bst

import (
	"math/rand"
	"testing"
	"time"

	"github.com/atkhx/ddb/pkg/key"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	item := New()
	assert.NotNil(t, item)
	assert.Nil(t, item.key)
	assert.Nil(t, item.data)
	assert.Nil(t, item.left)
	assert.Nil(t, item.right)
}

func TestItem_IsEmpty(t *testing.T) {
	t.Run("TrueOnEmptyKey", func(t *testing.T) {
		assert.True(t, New().IsEmpty())
	})

	t.Run("FalseOnFilledKey", func(t *testing.T) {
		item := New()
		item.key = key.IntKey(123)
		assert.False(t, item.IsEmpty())
	})
}

func TestItem_Search(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		assert.Nil(t, New().Search(key.IntKey(18)))
	})

	t.Run("currentItemEqualsKey", func(t *testing.T) {
		rootKey := key.IntKey(18)

		item := New()
		item.Insert(rootKey)
		assert.Equal(t, item, item.Search(rootKey))
	})

	t.Run("currentItemNotEqualsKeyNoChild", func(t *testing.T) {
		rootKey := key.IntKey(18)
		notRootKey := key.IntKey(19)

		item := New()
		item.Insert(rootKey)

		assert.Nil(t, item.Search(notRootKey))
	})

	t.Run("complexTest", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(5)
		rightKey := key.IntKey(21)

		notFoundMinInLeft := key.IntKey(3)
		notFoundMaxInLeft := key.IntKey(7)

		notFoundMinInRight := key.IntKey(19)
		notFoundMaxInRight := key.IntKey(23)

		item := New()
		rootItem := item.Insert(rootKey)
		leftItem := item.Insert(leftKey)
		rightItem := item.Insert(rightKey)

		assert.Equal(t, rootItem, item)
		assert.Equal(t, leftItem, item.Search(leftKey))
		assert.Equal(t, rightItem, item.Search(rightKey))

		assert.Nil(t, item.Search(notFoundMinInLeft))
		assert.Nil(t, item.Search(notFoundMaxInLeft))
		assert.Nil(t, item.Search(notFoundMinInRight))
		assert.Nil(t, item.Search(notFoundMaxInRight))
	})
}

func TestItem_Insert(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		item := New().Insert(key.IntKey(18))
		assert.True(t, !item.IsEmpty())
		assert.True(t, item.left.IsEmpty())
		assert.True(t, item.right.IsEmpty())
	})

	t.Run("duplicateKey", func(t *testing.T) {
		item := New().Insert(key.IntKey(18))
		actual := item.Insert(key.IntKey(18))
		assert.Equal(t, item, actual)
		assert.True(t, !item.IsEmpty())
		assert.True(t, item.left.IsEmpty())
		assert.True(t, item.right.IsEmpty())
	})

	t.Run("insertRight", func(t *testing.T) {
		rootKey := key.IntKey(18)
		rightKey := key.IntKey(28)
		rightRightKey := key.IntKey(30)

		item := New().Insert(rootKey)
		right := item.Insert(rightKey)
		rightRight := item.Insert(rightRightKey)

		assert.True(t, item.left.IsEmpty())
		assert.Equal(t, right, item.right)
		assert.Equal(t, rightRight, item.right.right)
	})

	t.Run("insertLeft", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(16)
		leftLeftKey := key.IntKey(10)

		item := New().Insert(rootKey)
		left := item.Insert(leftKey)
		leftLeft := item.Insert(leftLeftKey)

		assert.True(t, item.right.IsEmpty())
		assert.Equal(t, left, item.left)
		assert.Equal(t, leftLeft, item.left.left)
	})
}

func TestItem_Delete(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		assert.False(t, New().Delete(key.IntKey(19)))
	})

	t.Run("deleteRootWithoutChild", func(t *testing.T) {
		rootKey := key.IntKey(18)

		item := New()
		item.Insert(rootKey)
		assert.True(t, item.Delete(rootKey))
		assert.True(t, item.IsEmpty())
	})

	t.Run("deleteRight", func(t *testing.T) {
		rootKey := key.IntKey(18)
		rightKey := key.IntKey(26)

		item := New()
		item.Insert(rootKey)
		right := item.Insert(rightKey)

		assert.True(t, item.Delete(rightKey))
		assert.False(t, item.IsEmpty())
		assert.True(t, right.IsEmpty())

		assert.True(t, item.right.IsEmpty())
		assert.True(t, item.left.IsEmpty())
	})

	t.Run("deleteLeft", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(7)

		item := New()
		item.Insert(rootKey)
		left := item.Insert(leftKey)

		assert.True(t, item.Delete(leftKey))
		assert.False(t, item.IsEmpty())
		assert.True(t, left.IsEmpty())

		assert.True(t, item.right.IsEmpty())
		assert.True(t, item.left.IsEmpty())
	})

	t.Run("deleteRootWithRightChildren", func(t *testing.T) {
		rootKey := key.IntKey(18)
		rightKey := key.IntKey(26)

		item := New()
		item.Insert(rootKey)
		right := item.Insert(rightKey)

		assert.True(t, item.Delete(rootKey))
		assert.False(t, item.IsEmpty())
		assert.Equal(t, right, item)

		assert.True(t, item.right.IsEmpty())
		assert.True(t, item.left.IsEmpty())
	})

	t.Run("deleteRootWithLeftChildren", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(3)

		item := New()
		item.Insert(rootKey)
		left := item.Insert(leftKey)

		assert.True(t, item.Delete(rootKey))
		assert.False(t, item.IsEmpty())
		assert.Equal(t, left, item)

		assert.True(t, item.right.IsEmpty())
		assert.True(t, item.left.IsEmpty())
	})

	t.Run("deleteRootWithBothChildAndRightLeftIsEmpty", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(3)
		rightKey := key.IntKey(27)
		rightRightKey := key.IntKey(40)

		item := New()
		item.Insert(rootKey)

		left := item.Insert(leftKey)
		right := item.Insert(rightKey)
		rightRight := item.Insert(rightRightKey)

		assert.True(t, item.Delete(rootKey))
		assert.False(t, item.IsEmpty())

		// now root is right
		assert.Equal(t, right, item)
		// root.left saved in right.left
		assert.Equal(t, left, right.left)
		// rightRight point in right struct
		assert.Equal(t, rightRight, right.right)
	})

	t.Run("deleteRootWithBothChildAndMinInRightSubtreeHasNoRightChildren", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(3)
		rightKey := key.IntKey(27)
		rightLeftKey := key.IntKey(20)
		rightRightKey := key.IntKey(40)

		item := New()
		item.Insert(rootKey)

		left := item.Insert(leftKey)
		right := item.Insert(rightKey)
		rightLeft := item.Insert(rightLeftKey)
		rightRight := item.Insert(rightRightKey)

		assert.True(t, item.Delete(rootKey))
		assert.False(t, item.IsEmpty())

		// now root key equals min key in right subtree
		assert.Equal(t, rightLeftKey, item.key)
		// now min item in right subtree is empty, because it has no child
		assert.True(t, rightLeft.IsEmpty())

		assert.Equal(t, left, item.left)
		assert.Equal(t, right, item.right)
		assert.Equal(t, rightRight, item.right.right)
		assert.True(t, item.right.left.IsEmpty())
	})

	t.Run("deleteRootWithBothChildAndMinInRightSubtreeHasRightChildren", func(t *testing.T) {
		rootKey := key.IntKey(18)
		leftKey := key.IntKey(3)
		rightKey := key.IntKey(27)
		rightLeftKey := key.IntKey(20)
		rightLeftRightKey := key.IntKey(23)
		rightRightKey := key.IntKey(40)

		item := New()
		item.Insert(rootKey)

		left := item.Insert(leftKey)
		right := item.Insert(rightKey)
		rightLeft := item.Insert(rightLeftKey)
		rightRight := item.Insert(rightRightKey)
		rightLeftRight := item.Insert(rightLeftRightKey)

		assert.True(t, item.Delete(rootKey))
		assert.False(t, item.IsEmpty())

		// now root key equals min key in right subtree
		assert.Equal(t, rightLeftKey, item.key)
		// now min item in right subtree equals right child of rightLeft
		assert.Equal(t, rightLeftRight, rightLeft)

		assert.Equal(t, left, item.left)
		assert.Equal(t, right, item.right)
		assert.Equal(t, rightRight, item.right.right)
		assert.Equal(t, rightLeftRight, item.right.left)
	})
}

func TestItem_ScanAsc(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		var called bool
		New().ScanAsc(func(item *Item) bool {
			called = true
			return true
		})

		assert.False(t, called)
	})

	t.Run("itemWithoutChild", func(t *testing.T) {
		var called bool
		New().Insert(key.IntKey(11)).ScanAsc(func(item *Item) bool {
			called = true
			return true
		})

		assert.True(t, called)
	})

	t.Run("itemWithoutLeft", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(15))

		var callbackCalledTimes int
		item.ScanAsc(func(item *Item) bool {
			callbackCalledTimes++
			return true
		})

		assert.Equal(t, 2, callbackCalledTimes)
	})

	t.Run("itemWithoutRight", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(3))

		var callbackCalledTimes int
		item.ScanAsc(func(item *Item) bool {
			callbackCalledTimes++
			return true
		})

		assert.Equal(t, 2, callbackCalledTimes)
	})

	t.Run("sortItems", func(t *testing.T) {
		insertSortedKeys := []key.IntKey{16, 13, 20, 19, 25, 10, 15}
		expectedSortKeys := []key.IntKey{10, 13, 15, 16, 19, 20, 25}
		actualSortedKeys := []key.IntKey{}

		item := New()
		for _, k := range insertSortedKeys {
			item.Insert(k)
		}

		item.ScanAsc(func(item *Item) bool {
			actualSortedKeys = append(actualSortedKeys, item.key.(key.IntKey))
			return true
		})

		assert.Equal(t, expectedSortKeys, actualSortedKeys)
	})

	t.Run("limits", func(t *testing.T) {
		insertSortedKeys := []key.IntKey{16, 13, 20}

		item := New()
		for _, k := range insertSortedKeys {
			item.Insert(k)
		}

		assertFalseWithLimit := func(limit int) {
			assert.False(t, item.ScanAsc(func(item *Item) bool {
				if limit = limit - 1; limit == 0 {
					return false
				}
				return true
			}))
		}

		// left only
		assertFalseWithLimit(1)
		// left and root
		assertFalseWithLimit(2)
		// left, root and right
		assertFalseWithLimit(3)
	})
}

func TestItem_ScanDesc(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		var called bool
		New().ScanDesc(func(item *Item) bool {
			called = true
			return true
		})

		assert.False(t, called)
	})

	t.Run("itemWithoutChild", func(t *testing.T) {
		var called bool
		New().Insert(key.IntKey(11)).ScanDesc(func(item *Item) bool {
			called = true
			return true
		})

		assert.True(t, called)
	})

	t.Run("itemWithoutLeft", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(15))

		var callbackCalledTimes int
		item.ScanDesc(func(item *Item) bool {
			callbackCalledTimes++
			return true
		})

		assert.Equal(t, 2, callbackCalledTimes)
	})

	t.Run("itemWithoutRight", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(3))

		var callbackCalledTimes int
		item.ScanDesc(func(item *Item) bool {
			callbackCalledTimes++
			return true
		})

		assert.Equal(t, 2, callbackCalledTimes)
	})

	t.Run("sortItems", func(t *testing.T) {
		insertSortedKeys := []key.IntKey{16, 13, 20, 19, 25, 10, 15}
		expectedSortKeys := []key.IntKey{25, 20, 19, 16, 15, 13, 10}
		actualSortedKeys := []key.IntKey{}

		item := New()
		for _, k := range insertSortedKeys {
			item.Insert(k)
		}

		item.ScanDesc(func(item *Item) bool {
			actualSortedKeys = append(actualSortedKeys, item.key.(key.IntKey))
			return true
		})

		assert.Equal(t, expectedSortKeys, actualSortedKeys)
	})

	t.Run("limits", func(t *testing.T) {
		insertSortedKeys := []key.IntKey{16, 13, 20}

		item := New()
		for _, k := range insertSortedKeys {
			item.Insert(k)
		}

		assertFalseWithLimit := func(limit int) {
			assert.False(t, item.ScanDesc(func(item *Item) bool {
				if limit = limit - 1; limit == 0 {
					return false
				}
				return true
			}))
		}

		// right only
		assertFalseWithLimit(1)
		// right and root
		assertFalseWithLimit(2)
		// right, root and left
		assertFalseWithLimit(3)
	})
}

func TestItem_Min(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		assert.Nil(t, New().Min())
	})

	t.Run("itemWithoutChild", func(t *testing.T) {
		item := New().Insert(key.IntKey(11))
		assert.Equal(t, item, item.Min())
	})

	t.Run("itemWithoutLeft", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(18))

		assert.Equal(t, item, item.Min())
	})

	t.Run("itemWithoutRight", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		expected := item.Insert(key.IntKey(3))

		assert.Equal(t, expected, item.Min())
	})
}

func TestItem_Max(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		assert.Nil(t, New().Max())
	})

	t.Run("itemWithoutChild", func(t *testing.T) {
		item := New().Insert(key.IntKey(11))
		assert.Equal(t, item, item.Max())
	})

	t.Run("itemWithoutLeft", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		expected := item.Insert(key.IntKey(18))

		assert.Equal(t, expected, item.Max())
	})

	t.Run("itemWithoutRight", func(t *testing.T) {
		item := New()
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(3))

		assert.Equal(t, item, item.Max())
	})
}

func BenchmarkItem_Search(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	item := New()
	for i := 0; i < b.N; i++ {
		intVal := rand.Intn(b.N)
		item.Insert(key.IntKey(intVal))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intVal := rand.Intn(b.N)
		item.Search(key.IntKey(intVal))
	}
}

func BenchmarkItem_Insert(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	item := New()
	for i := 0; i < b.N; i++ {
		intVal := rand.Intn(b.N)
		item.Insert(key.IntKey(intVal))
	}
}
