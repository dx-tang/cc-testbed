package testbed

import (
	"fmt"
	"testing"
)

func TestStore(t *testing.T) {
	fmt.Println("=================")
	fmt.Println("Test Store Begin")
	fmt.Println("=================")
	// Test Partition-based CC
	*NumPart = 10
	*SysType = PARTITION
	s := NewStore()
	//Create Keys
	for i := 0; i < 100; i++ {
		k := Key(i)
		p := i % *NumPart
		s.CreateKV(k, int64(i*10), SINGLEINT, p)
	}
	key := Key(23)
	part := 23 % *NumPart
	//Get Key 23
	r := s.GetRecord(key, part)
	fmt.Printf("Original Value is %v \n", r.Value())
	//Update it
	s.SetRecord(key, int64(110), part)
	//Get it again
	fmt.Printf("Updated value is %v \n", s.GetRecord(key, part).Value())

	// Test string value
	stringList := []string{"totem", "tang", "aaron", "amy"}
	s = NewStore()
	for i := 0; i < 100; i++ {
		k := Key(i)
		p := i % *NumPart
		s.CreateKV(k, stringList, STRINGLIST, p)
	}
	key = Key(23)
	part = 23 % *NumPart
	//Get Key 23
	r = s.GetRecord(key, part)
	fmt.Printf("Original Value is %v \n", r.Value())
	//Update it
	strAttr := &StrAttr{
		index: 0,
		value: "totem-updated",
	}
	s.SetRecord(key, strAttr, part)
	//Get it again
	fmt.Printf("Updated value is %v \n", s.GetRecord(key, part).Value())

	fmt.Println("==============")
	fmt.Println("Test Store End")
	fmt.Println("==============\n")
}
