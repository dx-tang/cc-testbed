package testbed

import (
	"fmt"
	"testing"

	//	"github.com/totemtang/cc-testbed/clog"
)

func BenchmarkSmallBank(b *testing.B) {
	nParts := 4
	nWorkers := 4

	wl := NewSmallBankWL("benchmarks/smallbank/smallbank.txt", nParts, true, nWorkers, 1, "0:0:40:40:10:10", 50)
	s := wl.basic.store

	tg := wl.GetTransGen(1)

	tp := make([]int, 6)

	var count int
	var cp int
	var total float64

	for n := 0; n < b.N; n++ {
		t := tg.GenOneTrans()
		txn := t.GetTXN()
		tp[txn-SMALLBANKBASE-1]++

		ap := t.GetAccessParts()
		if len(ap) > 1 {
			cp++
		}
		count++

		sbTrans := t.(*SBTrans)

		var val Value
		var part int
		for i, key := range sbTrans.accoutID {
			if len(ap) > 1 {
				part = ap[i]
				val = s.GetValueByID(CHECKING, key, part, CHECK_BAL)
			} else {
				part = ap[0]
				val = s.GetValueByID(CHECKING, key, part, CHECK_BAL)
			}
			//if val == nil {
			//	clog.Error("key is %v; Part is %v\n", ParseKey(key, 0), part)
			//} else {
			total += *val.(*float64)
			//}
		}
	}

	for _, p := range tp {
		fmt.Printf("%v:", p)
	}
	fmt.Printf("\n")
	fmt.Printf("Cross Partiition Rate %.2f\n", float32(cp)/float32(count))
	fmt.Printf("Total Checking Value %.2f\n", total)

}
