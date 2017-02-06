package main

import (
	"fmt"
	"math/rand"
	"time"
)

var count int = 100

func main() {
	threshold := [2]int{70, 70}
	rnd0 := rand.New(rand.NewSource(time.Now().Unix()))
	rnd1 := rand.New(rand.NewSource(time.Now().Unix() + 100))
	for i := 0; i < 32; i++ {
		cur := [2]bool{true, false} // whether single-thread; whether read-only
		for j := 0; j < count; j++ {
			fmt.Printf("%v\t%v\n", cur[0], cur[1])
			if rnd0.Intn(100) > threshold[0] {
				cur[0] = !cur[0]
			}
			if rnd1.Intn(100) > threshold[1] {
				cur[1] = !cur[1]
			}
		}
		fmt.Printf("\n")
	}

}
