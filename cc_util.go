package testbed

import (
	crand "crypto/rand"

	"fmt"
	"github.com/totemtang/cc-testbed/clog"
)

func Randstr(sz int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, sz)
	crand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func printOneQuery(q *Query) {
	if !q.isPartition {
		fmt.Printf("This transaction touches whole store \n")
	} else {
		fmt.Printf("This transaction touches %v partitions: ", len(q.accessParts))
		for _, p := range q.accessParts {
			fmt.Printf("%v ", p)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("Read Keys Include: ")
	for _, k := range q.rKeys {
		fmt.Printf("%v ", k)
	}
	fmt.Printf("\nWrite Keys Include: ")
	for _, k := range q.wKeys {
		fmt.Printf("%v ", k)
	}
	fmt.Printf("\n")
	if q.TXN == RANDOM_UPDATE_INT {
		wValue := q.wValue.(*SingleIntValue)
		fmt.Printf("Write Values Include: ")
		for _, v := range wValue.intVals {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else if q.TXN == RANDOM_UPDATE_STRING {
		wValue := q.wValue.(*StringListValue)
		fmt.Printf("Write Values Include: \n")
		for _, v := range wValue.strVals {
			fmt.Printf("%v. %s \n", v.index, v.value)
		}
		fmt.Printf("\n")
	}

}

func PrintStore(s *Store, nKeys int64, p Partitioner) {
	for i := int64(0); i < nKeys; i++ {
		k := Key(i)
		partNum := p.GetPartition(k)
		r := s.GetRecord(k, partNum)
		if r == nil {
			clog.Error("Error No Key")
		}
		clog.Info("Key %v: %v", k, r.Value())
	}
}

func GenStringList() []string {
	ret := make([]string, FIELDS)
	for i := 0; i < FIELDS; i++ {
		ret[i] = Randstr(PERFIELD)
	}
	return ret
}
