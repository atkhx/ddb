package avl

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

func TestItem_Balance(t *testing.T) {
	type testCase struct {
		item    *Item
		balance int
	}

	testCases := map[string]testCase{
		"empty item": {
			item:    New(),
			balance: 0,
		},
		"leaf item": {
			item: func() *Item {
				item := New()
				item.height = 0
				return item
			}(),
			balance: 0,
		},
		"left only": {
			item: func() *Item {
				item := New()
				item.key = key.IntKey(2)
				item.height = 1

				item.right = New()
				item.left = New()
				item.left.key = key.IntKey(1)
				item.left.height = 0
				return item
			}(),
			balance: 1,
		},
		"right only": {
			item: func() *Item {
				item := New()
				item.key = key.IntKey(2)
				item.height = 1

				item.left = New()
				item.right = New()
				item.right.key = key.IntKey(4)
				item.right.height = 0
				return item
			}(),
			balance: -1,
		},
		"both child": {
			item: func() *Item {
				item := New()
				item.key = key.IntKey(2)
				item.height = 1

				item.right = New()
				item.right.key = key.IntKey(4)
				item.right.height = 0

				item.left = New()
				item.left.key = key.IntKey(1)
				item.left.height = 0
				return item
			}(),
			balance: 0,
		},
		"left & right.right": {
			item: func() *Item {
				item := New()
				item.key = key.IntKey(2)
				item.height = 2

				item.right = New()
				item.right.key = key.IntKey(8)
				item.right.height = 1

				item.right.right = New()
				item.right.right.key = key.IntKey(18)
				item.right.right.height = 0

				item.left = New()

				return item
			}(),
			balance: -2,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.balance, tc.item.Balance())
		})
	}
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
		item.Insert(rootKey)
		item.Insert(leftKey)
		item.Insert(rightKey)

		leftItem := item.Search(leftKey)
		rightItem := item.Search(rightKey)

		assert.NotNil(t, leftItem)
		assert.NotNil(t, rightItem)

		assert.NotNil(t, leftKey, leftItem.key)
		assert.Equal(t, rightKey, rightItem.key)

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
		assert.Equal(t, item, item.Insert(key.IntKey(18)))
		assert.True(t, !item.IsEmpty())
		assert.True(t, item.left.IsEmpty())
		assert.True(t, item.right.IsEmpty())
	})

	t.Run("insertWithMinLeftRotate", func(t *testing.T) {
		key18 := key.IntKey(18)
		key28 := key.IntKey(28)
		key30 := key.IntKey(30)

		item := New()
		item.Insert(key18)
		item.Insert(key28)
		// initiate min left rotate
		item.Insert(key30)

		assert.Equal(t, key28, item.key)
		assert.Equal(t, key18, item.left.key)
		assert.Equal(t, key30, item.right.key)

		assert.Equal(t, 1, item.height)
		assert.Equal(t, 0, item.left.height)
		assert.Equal(t, 0, item.right.height)
	})

	t.Run("insertWithMinRightRotate", func(t *testing.T) {
		key18 := key.IntKey(18)
		key16 := key.IntKey(16)
		key10 := key.IntKey(10)

		item := New()
		item.Insert(key18)
		item.Insert(key16)
		// initiate min right rotate
		item.Insert(key10)

		assert.Equal(t, key16, item.key)
		assert.Equal(t, key10, item.left.key)
		assert.Equal(t, key18, item.right.key)

		assert.Equal(t, 1, item.height)
		assert.Equal(t, 0, item.left.height)
		assert.Equal(t, 0, item.right.height)
	})

	t.Run("withBigLeftRotateLeft", func(t *testing.T) {
		item := New()

		item.Insert(key.IntKey(40))
		item.Insert(key.IntKey(30))
		item.Insert(key.IntKey(50))
		item.Insert(key.IntKey(45))
		item.Insert(key.IntKey(60))

		// initiate big left rotate
		item.Insert(key.IntKey(42))

		// check keys
		assert.Equal(t, key.IntKey(45), item.key)
		assert.Equal(t, key.IntKey(40), item.left.key)
		assert.Equal(t, key.IntKey(50), item.right.key)
		assert.Equal(t, key.IntKey(60), item.right.right.key)
		assert.Equal(t, key.IntKey(30), item.left.left.key)
		assert.Equal(t, key.IntKey(42), item.left.right.key)

		// check heights
		assert.Equal(t, 2, item.height)
		assert.Equal(t, 1, item.left.height)
		assert.Equal(t, 1, item.right.height)
		assert.Equal(t, 0, item.right.right.height)

		assert.Equal(t, 0, item.left.left.height)
		assert.Equal(t, 0, item.left.right.height)
	})

	t.Run("withBigLeftRotateRight", func(t *testing.T) {
		item := New()

		item.Insert(key.IntKey(40))
		item.Insert(key.IntKey(30))
		item.Insert(key.IntKey(50))
		item.Insert(key.IntKey(45))
		item.Insert(key.IntKey(60))
		// initiate big left rotate
		item.Insert(key.IntKey(47))

		// check keys
		assert.Equal(t, key.IntKey(45), item.key)
		assert.Equal(t, key.IntKey(40), item.left.key)
		assert.Equal(t, key.IntKey(50), item.right.key)
		assert.Equal(t, key.IntKey(60), item.right.right.key)
		assert.Equal(t, key.IntKey(47), item.right.left.key)
		assert.Equal(t, key.IntKey(30), item.left.left.key)

		// check heights
		assert.Equal(t, 2, item.height)
		assert.Equal(t, 1, item.left.height)
		assert.Equal(t, 1, item.right.height)
		assert.Equal(t, 0, item.right.right.height)
		assert.Equal(t, 0, item.right.left.height)
		assert.Equal(t, 0, item.left.left.height)
	})

	t.Run("withBigRightRotateLeft", func(t *testing.T) {
		item := New()

		item.Insert(key.IntKey(70))
		item.Insert(key.IntKey(60))
		item.Insert(key.IntKey(80))
		item.Insert(key.IntKey(50))
		item.Insert(key.IntKey(65))
		// initiate big right rotate
		item.Insert(key.IntKey(62))

		// check keys
		assert.Equal(t, key.IntKey(65), item.key)
		assert.Equal(t, key.IntKey(60), item.left.key)
		assert.Equal(t, key.IntKey(70), item.right.key)
		assert.Equal(t, key.IntKey(50), item.left.left.key)
		assert.Equal(t, key.IntKey(62), item.left.right.key)
		assert.Equal(t, key.IntKey(80), item.right.right.key)

		// check heights
		assert.Equal(t, 2, item.height)
		assert.Equal(t, 1, item.left.height)
		assert.Equal(t, 1, item.right.height)
		assert.Equal(t, 0, item.left.left.height)
		assert.Equal(t, 0, item.left.right.height)
		assert.Equal(t, 0, item.right.right.height)
	})

	t.Run("withBigRightRotateRight", func(t *testing.T) {
		item := New()

		item.Insert(key.IntKey(70))
		item.Insert(key.IntKey(60))
		item.Insert(key.IntKey(80))
		item.Insert(key.IntKey(50))
		item.Insert(key.IntKey(65))
		// initiate big right rotate
		item.Insert(key.IntKey(67))

		// check keys
		assert.Equal(t, key.IntKey(65), item.key)
		assert.Equal(t, key.IntKey(60), item.left.key)
		assert.Equal(t, key.IntKey(70), item.right.key)
		assert.Equal(t, key.IntKey(50), item.left.left.key)
		assert.Equal(t, key.IntKey(67), item.right.left.key)
		assert.Equal(t, key.IntKey(80), item.right.right.key)

		// check heights
		assert.Equal(t, 2, item.height)
		assert.Equal(t, 1, item.left.height)
		assert.Equal(t, 1, item.right.height)
		assert.Equal(t, 0, item.left.left.height)
		assert.Equal(t, 0, item.right.left.height)
		assert.Equal(t, 0, item.right.right.height)
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
		key18 := key.IntKey(18)
		key3 := key.IntKey(3)
		key27 := key.IntKey(27)
		key20 := key.IntKey(20)
		key40 := key.IntKey(40)

		item := New()
		item.Insert(key18)

		item.Insert(key3)
		item.Insert(key27)
		item.Insert(key20)
		item.Insert(key40)

		assert.True(t, item.Delete(key18))

		assert.Equal(t, key27, item.key)
		assert.Equal(t, key3, item.left.key)
		assert.Equal(t, key20, item.left.right.key)
		assert.Equal(t, key40, item.right.key)

		assert.Equal(t, 2, item.height)
		assert.Equal(t, 1, item.left.height)
		assert.Equal(t, 0, item.left.right.height)
		assert.Equal(t, 0, item.right.height)
	})
}

func TestItem_MinLeftRotate(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		assert.False(t, New().MinLeftRotate())
	})

	t.Run("emptyRight", func(t *testing.T) {
		assert.False(t, New().Insert(key.IntKey(16)).MinLeftRotate())
	})

	t.Run("hasRight", func(t *testing.T) {
		item := New()
		//     11          !         15
		// 10       15     !     11      17
		//       12   17   !  10   12
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(10))
		item.Insert(key.IntKey(15))
		item.Insert(key.IntKey(12))
		item.Insert(key.IntKey(17))

		assert.Equal(t, 2, item.height)
		assert.Equal(t, 0, item.left.height)
		assert.Equal(t, 1, item.right.height)
		assert.Equal(t, 0, item.right.left.height)
		assert.Equal(t, 0, item.right.right.height)

		assert.True(t, item.MinLeftRotate())

		assert.Equal(t, key.IntKey(15), item.key)
		assert.Equal(t, key.IntKey(11), item.left.key)
		assert.Equal(t, key.IntKey(17), item.right.key)
		assert.Equal(t, key.IntKey(10), item.left.left.key)
		assert.Equal(t, key.IntKey(12), item.left.right.key)

		assert.Equal(t, 2, item.height)
		assert.Equal(t, 1, item.left.height)
		assert.Equal(t, 0, item.right.height)
		assert.Equal(t, 0, item.left.left.height)
		assert.Equal(t, 0, item.left.right.height)
	})
}

func TestItem_RotateRight(t *testing.T) {
	t.Run("emptyItem", func(t *testing.T) {
		assert.False(t, New().MinRightRotate())
	})

	t.Run("emptyLeft", func(t *testing.T) {
		assert.False(t, New().Insert(key.IntKey(16)).MinRightRotate())
	})

	t.Run("hasLeft", func(t *testing.T) {
		item := New()
		//          15       !       11
		//      11      17   !    10    15
		//   10   12         !        12  17
		item.Insert(key.IntKey(15))
		item.Insert(key.IntKey(11))
		item.Insert(key.IntKey(17))
		item.Insert(key.IntKey(10))
		item.Insert(key.IntKey(12))

		assert.True(t, item.MinRightRotate())

		assert.Equal(t, key.IntKey(11), item.key)
		assert.Equal(t, key.IntKey(10), item.left.key)
		assert.Equal(t, key.IntKey(15), item.right.key)

		assert.Equal(t, key.IntKey(12), item.right.left.key)
		assert.Equal(t, key.IntKey(17), item.right.right.key)
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

func BenchmarkItem_Delete(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	item := New()
	for i := 0; i < b.N; i++ {
		intVal := rand.Intn(b.N)
		item.Insert(key.IntKey(intVal))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intVal := rand.Intn(b.N)
		item.Delete(key.IntKey(intVal))
	}
}
