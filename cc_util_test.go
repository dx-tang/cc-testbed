package testbed

import (
	"fmt"
	"testing"
)

func TestRandN(t *testing.T) {
	fmt.Println("================")
	fmt.Println("Test RandN Begin")
	fmt.Println("================")

	/*
		seed := uint32(7432)

		var small int = 0
		var x uint32

		for i := 0; i < 1000; i++ {
			x = RandN(&seed, 100)
			//fmt.Printf("Random %v \n", x)
			if x < 50 {
				small++
			}
		}

		fmt.Printf("Not Bigger Than 50 is %v \n", small)
	*/
	fmt.Println("==============")
	fmt.Println("Test RandN End")
	fmt.Println("==============")

}
