package testbed

import (
	"fmt"
	"math"
	"testing"
	"time"
)

type AR struct {
	padding1 [PADDING]byte
	key      []Key
	count    []int
	padding2 [PADDING]byte
}

func TestZipf(t *testing.T) {
/*
	numKeys := int64(10000000)
	theta := 0.99
	zipf := NewZipfGenerator(numKeys, theta, 0)
	//a := []int{0, 0, 0, 0}
	m := 1000
	tc := NewTopKCounter(m, 100)
	keyAR := make([]Key, m)
	countAR := make([]int, m)
	for j := 0; j < 2; j++ {
		var k Key
		for i := 0; i < 2000; i++ {
			k[0] = int(zipf.NextInt())
			//fmt.Printf("Insert %v\n", k[0])
			tc.InsertKey(k)
		}
		n := tc.GetTopK(keyAR, countAR, 100)
		fmt.Printf("%v", n)
		for i := 0; i < n; i++ {
			fmt.Printf("(%v, %v) ", keyAR[i][0], countAR[i])
		}
		fmt.Println()
		tc.Reset()
	}
*/
	
		BIGK := 1000
		Cores := 100
		ARCollect := make([]AR, Cores)
		for i, _ := range ARCollect {
			arPoint := &ARCollect[i]
			arPoint.count = make([]int, BIGK)
			arPoint.key = make([]Key, BIGK)
			for j, _ := range arPoint.count {
				arPoint.count[j] = 1
				arPoint.key[j][0] = 1
			}
		}


			start := time.Now()
			total := 0
			for i, _ := range ARCollect {
				A := &ARCollect[i]
				probe := make(map[Key]int)
				for j, _ := range A.key {
					probe[A.key[j]] = A.count[j]
				}
				for j := i + 1; j < Cores; j++ {
					B := &ARCollect[j]
					for q := 0; q < BIGK; q++ {
						tmpCount, ok := probe[B.key[q]]
						if ok {
							total += int(math.Min(float64(tmpCount), float64(B.count[q])))
						}
					}
				}
			}

			fmt.Printf("Take %v %.4f \n", total, time.Since(start).Seconds())
	

}
