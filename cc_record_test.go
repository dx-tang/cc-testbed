package testbed

import (
	"fmt"
	"testing"
)

func TestRecord(t *testing.T) {

	// test MakeBR
	var k Key
	var intValue int64 = 10
	var br = MakeBR(k, intValue, SINGLEINT)
	fmt.Printf("Int Value is %v \n", br.Value())

	var stringList = []string{"Hello", "World"}
	br = MakeBR(k, stringList, STRINGLIST)
	fmt.Printf("String Value is %v \n", br.Value())

	// test lock
	if *SysType == PARTITION {
		*SysType = OCC
	}

	br = MakeBR(k, stringList, STRINGLIST)

	br.RLock()
	br.RUnlock()
	br.Lock()
	br.Unlock()
}
