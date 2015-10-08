package testbed

import (
	"fmt"
	"testing"
)

func TestStore(t *testing.T) {
	// Test Partition-based CC
	*NumPart = 10
	*SysType = PARTITION
	s := NewStore()
	//Create Keys
	for i := 0; i < 100; i++ {
		k := CKey(int64(i))
		p := i % *NumPart
		s.CreateKV(k, int64(i*10), SINGLEINT, p)
	}
	key := CKey(int64(23))
	part := 23 % *NumPart
	//Get Key 23
	br := s.GetRecord(key, part)
	fmt.Printf("Original Value is %v \n", br.Value())
	//Update it
	s.SetRecord(key, int64(110), part)
	//Get it again
	fmt.Printf("Updated value is %v \n", s.GetRecord(key, part).Value())

	// Test Other CC with string value
	*SysType = OCC
	stringList := []string{"totem", "tang", "aaron", "amy"}
	s = NewStore()
	for i := 0; i < 100; i++ {
		k := CKey(int64(i))
		p := i % *NumPart
		s.CreateKV(k, stringList, STRINGLIST, p)
	}
	key = CKey(int64(23))
	part = 23 % *NumPart
	//Get Key 23
	br = s.GetRecord(key, part)
	fmt.Printf("Original Value is %v \n", br.Value())
	//Update it
	strAttr := &StrAttr{
		index: 0,
		value: "totem-updated",
	}
	s.SetRecord(key, strAttr, part)
	//Get it again
	fmt.Printf("Updated value is %v \n", s.GetRecord(key, part).Value())
}
