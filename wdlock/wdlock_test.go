package wdlock

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	//"time"
)

func TestWDLock(t *testing.T) {
	/*rw := WDLock{
		l: maxreaders,
	}*/
	var rw WDLock
	rw.Initialize()
	//rw.l = maxreaders
	rw2 := WDLock{
		l: maxreaders,
	}
	var tid uint64 = 0

	ngo := 10
	locked := make([]int, ngo)
	unlocked := make([]int, ngo)

	var wg sync.WaitGroup
	for i := 0; i < ngo; i++ {
		wg.Add(1)
		go func(id int) {
			//done := time.NewTimer(time.Duration(1 * time.Second)).C
			for {
				localtid := atomic.AddUint64(&tid, 1)
				/*select {
				case <-done:
					break
				default:*/
				if localtid > 100000 {
					break
				}
				if id >= 5 {
					ok := rw.RLock(localtid)
					if ok {
						locked[id]++
						if !rw.IsRlocked() {
							t.Errorf("I rlocked it! %x %x\n", id, rw.l)
						}
						if rw.Upgrade(localtid) {
							if !rw.IsLocked() {
								t.Errorf("I upgraded it! %x %x\n", id, rw.l)
							}
							ok := rw2.Lock(localtid)
							if ok {
								if !rw2.IsLocked() {
									t.Errorf("I locked it 2! %x %x\n", id, rw2.l)
								}
								rw2.Unlock()
							}
							rw.Unlock()
						}
						//rw.RUnlock()
						unlocked[id]++
					}
				} else {
					ok := rw.Lock(localtid)
					if ok {
						locked[id]++
						if !rw.IsLocked() {
							t.Errorf("I locked it! %x %x\n", id, rw.l)
						}

						ok := rw2.RLock(localtid)
						if ok {
							if !rw2.IsRlocked() {
								t.Errorf("I rlocked it 2! %x %x\n", id, rw2.l)
							}
							rw2.RUnlock()
						}

						rw.Unlock()
						unlocked[id]++
					}
				}

				//}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	total_locked := 0
	total_unlocked := 0
	for i := 0; i < ngo; i++ {
		total_locked += locked[i]
		total_unlocked += unlocked[i]
	}
	fmt.Printf("TID %v; tid %v, Locked %v\n", rw.GetTid(), tid, total_locked)
	if total_locked != total_unlocked {
		t.Errorf("Mismatched %v %v %x\n", total_locked, total_unlocked, rw.l)
	}
}
