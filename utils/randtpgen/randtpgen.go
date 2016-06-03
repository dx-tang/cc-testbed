package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	tp := make([]int, 6)
	for i := 0; i < 30; i++ {
		sum := 0
		for j := 0; j < 6; j++ {
			tp[j] = rnd.Intn(10)
			sum += tp[j]
		}
		for j := 0; j < 5; j++ {
			tp[j] = int((float64(tp[j]) / float64(sum)) * 100)
		}
		tp[5] = 100
		for j := 0; j < 5; j++ {
			tp[5] -= tp[j]
		}
		var retStr string
		for k := 0; k < len(tp)-1; k++ {
			retStr += strconv.Itoa(tp[k]) + ":"
		}
		retStr += strconv.Itoa(tp[len(tp)-1])
		fmt.Printf("%s\n", retStr)
	}
}
