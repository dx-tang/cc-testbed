package testbed

import (
	"math/rand"
	"testing"
)

func BenchmarkStorePartition(b *testing.B) {
	nParts := 10
	s := NewStore("workload.txt", nParts)
	nAccounts := 10000
	key := make([]OneKey, 1)

	iv := &IntValue{}

	val1 := make([]Value, 2)
	sv := allocStrVal()
	sv.stringVal = sv.stringVal[:4]
	for i := 0; i < 4; i++ {
		sv.stringVal[i] = "name"[i]
	}
	val1[0] = iv
	val1[1] = sv

	val2 := make([]Value, 2)
	fv := &FloatValue{
		floatVal: float64(100.0),
	}
	val2[0] = iv
	val2[1] = fv

	// create table ACCOUNTS
	for i := 0; i < nAccounts; i++ {
		key[0] = OneKey(i)
		partNum := i % *NumPart
		iv.intVal = int64(i)
		s.CreateRecByName("ACCOUNTS", CKey(key), partNum, val1)
		s.CreateRecByName("SAVINGS", CKey(key), partNum, val2)
		s.CreateRecByName("CHECKING", CKey(key), partNum, val2)
	}

	rnd := rand.New(rand.NewSource(1))

	var inputKey Key
	rndKey := make([]OneKey, 1)

	for n := 0; n < b.N; n++ {
		rndKey[0] = OneKey(rnd.Intn(nAccounts))
		partNum := int(rndKey[0]) % *NumPart
		UKey(rndKey, &inputKey)

		// Substract 10 from checking acccount
		val := s.GetValueByName("CHECKING", inputKey, partNum, 1)
		fv.floatVal = val.(*FloatValue).floatVal - float64(10.0)
		s.SetValueByName("CHECKING", inputKey, partNum, fv, 1)

		// Add 10 to saving account
		val = s.GetValueByName("SAVINGS", inputKey, partNum, 1)
		fv.floatVal = val.(*FloatValue).floatVal + float64(10.0)
		s.SetValueByName("SAVINGS", inputKey, partNum, fv, 1)
	}
}

func BenchmarkStoreOCC(b *testing.B) {
	nParts := 1
	s := NewStore("workload.txt", nParts)
	nAccounts := 100
	key := make([]OneKey, 1)

	iv := &IntValue{}

	val1 := make([]Value, 2)
	sv := allocStrVal()
	sv.stringVal = sv.stringVal[:4]
	for i := 0; i < 4; i++ {
		sv.stringVal[i] = "name"[i]
	}
	val1[0] = iv
	val1[1] = sv

	val2 := make([]Value, 2)
	fv := &FloatValue{
		floatVal: float64(100.0),
	}
	val2[0] = iv
	val2[1] = fv

	// create table ACCOUNTS
	for i := 0; i < nAccounts; i++ {
		key[0] = OneKey(i)
		partNum := 0
		iv.intVal = int64(i)
		s.CreateRecByName("ACCOUNTS", CKey(key), partNum, val1)
		s.CreateRecByName("SAVINGS", CKey(key), partNum, val2)
		s.CreateRecByName("CHECKING", CKey(key), partNum, val2)
	}

	rnd := rand.New(rand.NewSource(1))

	var inputKey Key
	rndKey := make([]OneKey, 1)

	for n := 0; n < b.N; n++ {
		rndKey[0] = OneKey(rnd.Intn(nAccounts))
		partNum := 0
		UKey(rndKey, &inputKey)

		// Substract 10 from checking acccount
		val := s.GetValueByName("CHECKING", inputKey, partNum, 1)
		fv.floatVal = val.(*FloatValue).floatVal - float64(10.0)
		s.SetValueByName("CHECKING", inputKey, partNum, fv, 1)

		// Add 10 to saving account
		val = s.GetValueByName("SAVINGS", inputKey, partNum, 1)
		fv.floatVal = val.(*FloatValue).floatVal + float64(10.0)
		s.SetValueByName("SAVINGS", inputKey, partNum, fv, 1)
	}
}
