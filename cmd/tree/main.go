package main

import (
	"fmt"

	"github.com/atkhx/ddb/internal/keys"
	"github.com/atkhx/ddb/pkg/btree"
)

func main() {
	tree := btree.NewTree(3, btree.NewInmemProvider())
	cnt := 15

	showTree := func(cnt int) {
		for i := 0; i < cnt; i++ {
			userId := keys.StrKey(fmt.Sprintf("user_%d", i))
			fmt.Println(userId, tree.Get(userId))
		}
	}

	for i := 0; i < cnt; i++ {
		tree.Set(keys.StrKey(fmt.Sprintf("user_%d", i)), fmt.Sprintf("value for user %d", i))
		showTree(i)
		fmt.Println()
	}

}

func main2() {
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
}
