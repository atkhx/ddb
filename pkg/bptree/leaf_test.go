package bptree

import (
	"testing"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/keys"
	"github.com/stretchr/testify/assert"
)

func TestLeaf_Get(t *testing.T) {
	key1 := keys.IntKey(1)
	leaf := NewLeaf(10)

	t.Run("get not existed key", func(t *testing.T) {
		assert.Nil(t, leaf.Get(key1))
	})

	t.Run("get not existed key", func(t *testing.T) {
		leaf.Set(key1, "some row")
		assert.Equal(t, "some row", leaf.Get(key1))
	})
}

func TestLeaf_Set(t *testing.T) {
	key1 := keys.IntKey(1)
	key2 := keys.IntKey(2)
	key3 := keys.IntKey(3)

	leaf := NewLeaf(10)

	t.Run("set new key", func(t *testing.T) {
		leaf.Set(key2, "row 2 first")
		assert.Equal(t, "row 2 first", leaf.Get(key2))
	})

	t.Run("set existed key", func(t *testing.T) {
		leaf.Set(key2, "row 2")
		assert.Equal(t, "row 2", leaf.Get(key2))
	})

	t.Run("set less key", func(t *testing.T) {
		leaf.Set(key1, "row 1")
		assert.Equal(t, "row 1", leaf.Get(key1))
		assert.Equal(t, "row 2", leaf.Get(key2))
	})

	t.Run("set greater key", func(t *testing.T) {
		leaf.Set(key3, "row 3")
		assert.Equal(t, "row 1", leaf.Get(key1))
		assert.Equal(t, "row 2", leaf.Get(key2))
		assert.Equal(t, "row 3", leaf.Get(key3))
	})
}

func TestLeaf_Split(t *testing.T) {
	t.Run("no need split", func(t *testing.T) {
		originLeaf := &leaf{
			keys: []internal.Key{keys.IntKey(1), keys.IntKey(2), keys.IntKey(3)},
			rows: []internal.Row{"row 1", "row 2", "row 3"},
			cap:  4,
		}

		key, item := originLeaf.Split()

		assert.Nil(t, key)
		assert.Nil(t, item)
	})

	t.Run("split on cap 3", func(t *testing.T) {
		originNextLeaf := &leaf{
			keys: []internal.Key{keys.IntKey(4), keys.IntKey(5), keys.IntKey(6)},
			rows: []internal.Row{"row 4", "row 5", "row 6"},
			cap:  3,
		}

		originLeaf := &leaf{
			keys: []internal.Key{keys.IntKey(1), keys.IntKey(2), keys.IntKey(3)},
			rows: []internal.Row{"row 1", "row 2", "row 3"},
			cap:  3,
			next: originNextLeaf,
		}

		key, item := originLeaf.Split()

		assert.Equal(t, keys.IntKey(3), key)
		assert.Equal(t, &leaf{
			keys: []internal.Key{keys.IntKey(3)},
			rows: []internal.Row{"row 3"},
			cap:  3,
			next: originNextLeaf,
		}, item)

		assert.Equal(t, &leaf{
			keys: []internal.Key{keys.IntKey(1), keys.IntKey(2)},
			rows: []internal.Row{"row 1", "row 2"},
			cap:  3,
			next: item.(*leaf),
		}, originLeaf)
	})

	t.Run("split on cap 4", func(t *testing.T) {
		originNextLeaf := &leaf{
			keys: []internal.Key{keys.IntKey(5), keys.IntKey(6), keys.IntKey(7)},
			rows: []internal.Row{"row 5", "row 6", "row 7"},
			cap:  4,
		}

		originLeaf := &leaf{
			keys: []internal.Key{keys.IntKey(1), keys.IntKey(2), keys.IntKey(3), keys.IntKey(4)},
			rows: []internal.Row{"row 1", "row 2", "row 3", "row 4"},
			cap:  4,
			next: originNextLeaf,
		}

		key, item := originLeaf.Split()

		assert.Equal(t, keys.IntKey(3), key)
		assert.Equal(t, &leaf{
			keys: []internal.Key{keys.IntKey(3), keys.IntKey(4)},
			rows: []internal.Row{"row 3", "row 4"},
			cap:  4,
			next: originNextLeaf,
		}, item)

		assert.Equal(t, &leaf{
			keys: []internal.Key{keys.IntKey(1), keys.IntKey(2)},
			rows: []internal.Row{"row 1", "row 2"},
			cap:  4,
			next: item.(*leaf),
		}, originLeaf)
	})
}
