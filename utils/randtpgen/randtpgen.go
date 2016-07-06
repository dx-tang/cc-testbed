package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

const (
	TRANNUM = 6
)

func main() {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	tp := make([]int, TRANNUM)
	for i := 0; i < 30; i++ {
		sum := 0
		tp[TRANNUM-1] = rnd.Intn(5)
		remain := 100 - tp[TRANNUM-1]
		for j := 0; j < TRANNUM-1; j++ {
			tp[j] = rnd.Intn(10)
			sum += tp[j]
		}
		for j := 0; j < TRANNUM-2; j++ {
			tp[j] = int((float64(tp[j]) / float64(sum)) * float64(remain))
		}
		tp[TRANNUM-2] = remain
		for j := 0; j < TRANNUM-2; j++ {
			tp[TRANNUM-2] -= tp[j]
		}
		var retStr string
		for k := 0; k < len(tp)-1; k++ {
			retStr += strconv.Itoa(tp[k]) + ":"
		}
		//retStr += (strconv.Itoa(tp[len(tp)-1]) + ":0")
		retStr += (strconv.Itoa(tp[len(tp)-1]))
		fmt.Printf("%s\n", retStr)
	}
}
