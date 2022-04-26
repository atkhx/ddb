package graph

import (
	"fmt"
	"log"
	"testing"
)

func TestShortPath(t *testing.T) {
	metro1 := &vertex{name: "алтуфьево"}
	metro2 := &vertex{name: "отрадное"}
	metro3 := &vertex{name: "владыкино"}
	metro4 := &vertex{name: "новослободская"}
	metro5 := &vertex{name: "ботанический сад"}
	metro6 := &vertex{name: "алексеевская"}
	metro7 := &vertex{name: "медведково"}

	mck1 := &vertex{name: "мцк владыкино"}
	mck2 := &vertex{name: "мцк ботанический сад"}

	metro1.edges = []*edge{{to: metro2}}
	metro2.edges = []*edge{{to: metro3}}
	metro3.edges = []*edge{{to: metro4}, {to: mck1}}
	metro5.edges = []*edge{{to: metro6}, {to: metro7}}
	metro6.edges = []*edge{{to: metro4}}

	metro7.edges = []*edge{{to: metro1}}

	mck1.edges = []*edge{{to: mck2}}
	mck2.edges = []*edge{{to: metro5}}

	paths, ok := GetPaths(metro1, metro4)

	log.Println("ok", ok)
	log.Println("len(paths)", len(paths))
	log.Println("paths", paths)

	for i := 0; i < len(paths); i++ {
		fmt.Println("path", i)
		for j := 0; j < len(paths[i]); j++ {
			fmt.Println("-", paths[i][j].name)
		}
	}

}
