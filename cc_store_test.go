package testbed

import (
	"math/rand"
	"testing"
)

func BenchmarkStore(b *testing.B) {
	nParts := 10
	s := NewStore("workload.txt", nParts)
	nAccounts := 10000

	key := make([]OneKey, 1)
	// create table ACCOUNTS
	for i := 0; i < nAccounts; i++ {
		key[0] = OneKey(i)
		partNum := i % *NumPart

		at := &AccoutsTuple{
			accoutID: int64(i),
			name:     make([]byte, 0, SBSTRMAXLEN+2*PADDINGBYTE),
		}
		at.name = at.name[PADDINGBYTE:PADDINGBYTE]
		at.name = at.name[:4]
		for p := 0; p < 4; p++ {
			at.name[p] = "name"[p]
		}
		s.CreateRecByID(ACCOUNTS, CKey(key), partNum, at)

		st := &SavingsTuple{
			accoutID: int64(i),
			balance:  float64(BAL),
		}
		s.CreateRecByID(SAVINGS, CKey(key), partNum, st)

		ct := &CheckingTuple{
			accoutID: int64(i),
			balance:  float64(BAL),
		}
		s.CreateRecByID(CHECKING, CKey(key), partNum, ct)
	}

	rnd := rand.New(rand.NewSource(1))

	var inputKey Key
	rndKey := make([]OneKey, 1)

	fv := &FloatValue{}
	for n := 0; n < b.N; n++ {
		rndKey[0] = OneKey(rnd.Intn(nAccounts))
		partNum := int(rndKey[0]) % *NumPart
		UKey(rndKey, &inputKey)

		// Substract 10 from checking acccount
		val := s.GetValueByID(CHECKING, inputKey, partNum, CHECK_BAL)
		fv.floatVal = *val.(*float64) - float64(10.0)
		s.SetValueByID(CHECKING, inputKey, partNum, fv, CHECK_BAL)

		// Add 10 to saving account
		val = s.GetValueByID(SAVINGS, inputKey, partNum, SAVING_BAL)
		fv.floatVal = *val.(*float64) + float64(10.0)
		s.SetValueByID(SAVINGS, inputKey, partNum, fv, SAVING_BAL)
	}
}

func BenchmarkStoreLock(b *testing.B) {
	*SysType = LOCKING
	nParts := 10
	s := NewStore("workload.txt", nParts)
	nAccounts := 10000

	key := make([]OneKey, 1)
	// create table ACCOUNTS
	for i := 0; i < nAccounts; i++ {
		key[0] = OneKey(i)
		partNum := i % *NumPart

		at := &AccoutsTuple{
			accoutID: int64(i),
			name:     make([]byte, 0, SBSTRMAXLEN+2*PADDINGBYTE),
		}
		at.name = at.name[PADDINGBYTE:PADDINGBYTE]
		at.name = at.name[:4]
		for p := 0; p < 4; p++ {
			at.name[p] = "name"[p]
		}
		s.CreateRecByID(ACCOUNTS, CKey(key), partNum, at)

		st := &SavingsTuple{
			accoutID: int64(i),
			balance:  float64(BAL),
		}
		s.CreateRecByID(SAVINGS, CKey(key), partNum, st)

		ct := &CheckingTuple{
			accoutID: int64(i),
			balance:  float64(BAL),
		}
		s.CreateRecByID(CHECKING, CKey(key), partNum, ct)
	}

	rnd := rand.New(rand.NewSource(1))

	var inputKey Key
	rndKey := make([]OneKey, 1)

	fv := &FloatValue{}
	for n := 0; n < b.N; n++ {
		rndKey[0] = OneKey(rnd.Intn(nAccounts))
		partNum := int(rndKey[0]) % *NumPart
		UKey(rndKey, &inputKey)

		// Substract 10 from checking acccount
		//val := s.GetValueByID(CHECKING, inputKey, partNum, CHECK_BAL)
		rec := s.GetRecByID(CHECKING, inputKey, partNum)

		for !rec.RLock() {

		}
		val := rec.GetValue(CHECK_BAL)
		fv.floatVal = *val.(*float64) - float64(10.0)

		for !rec.Upgrade() {

		}
		s.SetValueByID(CHECKING, inputKey, partNum, fv, CHECK_BAL)
		rec.WUnlock()

		// Add 10 to saving account
		//val = s.GetValueByID(SAVINGS, inputKey, partNum, SAVING_BAL)
		rec = s.GetRecByID(SAVINGS, inputKey, partNum)
		for !rec.WLock() {

		}
		fv.floatVal = *val.(*float64) + float64(10.0)
		s.SetValueByID(SAVINGS, inputKey, partNum, fv, SAVING_BAL)
		rec.WUnlock()
	}
}
