package testbed

import (
	"fmt"
	"testing"
)

func TestRecord(t *testing.T) {

	fmt.Println("=================")
	fmt.Println("Test Record Begin")
	fmt.Println("=================")
	// test MakeBR
	var k Key
	var intValue int64 = 10
	var r = MakeRecord(k, intValue, SINGLEINT)
	fmt.Printf("Int Value is %v \n", r.Value())

	var stringList = []string{"Hello", "World"}
	r = MakeRecord(k, stringList, STRINGLIST)
	fmt.Printf("String Value is %v \n", r.Value())

	fmt.Println("===============")
	fmt.Println("Test Record End")
	fmt.Println("===============\n")
}
