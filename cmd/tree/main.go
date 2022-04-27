package main

import (
	"fmt"

	"github.com/atkhx/ddb/internal"
	"github.com/atkhx/ddb/internal/keys"
	"github.com/atkhx/ddb/pkg/btree"
)

func main2() {
	tree := btree.NewTree(3, btree.NewInmemProvider())
	cnt := 15

	showTree := func(cnt int) {
		for i := 0; i < cnt; i++ {
			userId := keys.StrKey(fmt.Sprintf("user_%d", i))
			r, err := tree.Get(userId)
			if err != nil {
				fmt.Println(userId, "error", err)
			} else {
				fmt.Println(userId, r)
			}
		}
	}

	for i := 0; i < cnt; i++ {
		tree.Set(keys.StrKey(fmt.Sprintf("user_%d", i)), fmt.Sprintf("value for user %d", i))
		showTree(i)
		fmt.Println()
	}

}

func main() {
	tree := btree.NewTree(3, btree.NewInmemProvider())
	tree.Set(keys.StrKey("Вася"), "Василий 1")
	tree.Set(keys.StrKey("Вася"), "Василий 2")
	tree.Set(keys.StrKey("Вася"), "Василий 3")
	fmt.Println(tree.Get(keys.StrKey("Вася")))
	fmt.Println()
	tree.Set(keys.StrKey("Вася"), "Василий 4")
	tree.Set(keys.StrKey("Вася"), "Василий 5")
	fmt.Println(tree.Get(keys.StrKey("Вася")))
	fmt.Println()
	tree.Set(keys.StrKey("Вова"), "Владимир 1")
	tree.Set(keys.StrKey("Вова"), "Владимир 2")
	tree.Set(keys.StrKey("Вова"), "Владимир 3")
	tree.Set(keys.StrKey("Вова"), "Владимир 4")
	tree.Set(keys.StrKey("Вова"), "Владимир 5")
	tree.Set(keys.StrKey("Вова"), "Владимир 6")
	tree.Set(keys.StrKey("Вова"), "Владимир 7")
	tree.Set(keys.StrKey("Вова"), "Владимир 8")
	tree.Set(keys.StrKey("Вова"), "Владимир 9")
	tree.Set(keys.StrKey("Вова"), "Владимир 10")
	tree.Set(keys.StrKey("Вова"), "Владимир 11")
	tree.Set(keys.StrKey("Вова"), "Владимир 12")
	fmt.Println(tree.Get(keys.StrKey("Вася")))
	fmt.Println(tree.Get(keys.StrKey("Вова")))
	fmt.Println()
	tree.Set(keys.StrKey("Петя"), "Петр 1")
	tree.Set(keys.StrKey("Маша"), "Маша 1")
	fmt.Println(tree.Get(keys.StrKey("Петя")))
	fmt.Println(tree.Get(keys.StrKey("Маша")))
	fmt.Println(tree.Get(keys.StrKey("Вася")))
	fmt.Println(tree.Get(keys.StrKey("Вова")))
	fmt.Println()
	fmt.Println()
	fmt.Println()

	tree.ScanASC(func(row internal.Row) bool {
		fmt.Println(row)
		return false
	})

	tree.ScanDESC(func(row internal.Row) bool {
		fmt.Println(row)
		return false
	})
}
