package main

import (
	"fmt"

	"github.com/atkhx/ddb/internal/storage"
)

func main() {
	locks := map[storage.Key]int64{}

	fmt.Println("txid", locks["some"])

	d := &data{items: []int{10, 11, 12, 13, 14}}
	c := make(chan bool, 1)
	r := d.iterate(c, d.items)
	<-r
	d.items = d.items[4:]
	c <- true
	<-r
}

type data struct {
	items []int
}

func (d *data) iterate(c chan bool, _ []int) chan bool {
	var skip bool
	var done = make(chan bool, 1)
	var items = d.items

	go func() {
		for i := len(items); i > 0; i-- {
			if !skip {
				done <- true
				<-c
				skip = true
			}

			fmt.Println("i", i, "val", items[i-1])
		}
		done <- true
	}()

	return done
}
