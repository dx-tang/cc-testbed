package main

import (
	"fmt"
	"math/rand"
	"time"
)

var count int = 20

func main() {
	crRange := [9]int{0, 4, 8, 12, 16, 20, 24, 28, 32}
	CR := [2]int{50, 100}
	transMix := [10]string{"44:44:0:4:0:4:4", "40:4:25:0:16:10:5", "20:30:0:23:25:0:2", "38:0:0:29:25:5:3", "18:4:9:31:26:10:2", "35:16:21:14:2:8:4", "23:17:17:17:9:15:2", "36:27:13:3:15:4:2", "19:32:0:19:23:4:3", "50:20:0:30:0:0:0"}
	rnd0 := rand.New(rand.NewSource(time.Now().Unix()))
	rnd1 := rand.New(rand.NewSource(time.Now().Unix() + 100))
	rnd2 := rand.New(rand.NewSource(time.Now().Unix() + 1000))
	fmt.Printf("%v\n\n", count)
	for i := 0; i < count; i++ {
		temp1 := crRange[rnd0.Intn(len(crRange))]
		temp2 := 0
		if temp1 != 0 {
			temp2 = CR[rnd1.Intn(len(CR))]
		}
		temp3 := transMix[rnd2.Intn(len(transMix))]
		fmt.Printf("%v\t%v\t%v\n", temp2, temp1, temp3)
	}

}
