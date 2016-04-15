package testbed

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWDLock(t *testing.T) {
	/*rw := WDLock{
		l: maxreaders,
	}*/

	*NoWait = false
	*NumPart = 10

	// Initilize GlobleBuf
	globalBuf = make([]*LockReqBuffer, *NumPart)
	for i := 0; i < *NumPart; i++ {
		globalBuf[i] = NewLockReqBuffer(maxwaiters, i)
	}

	rec := &LRecord{}
	rec.wd.Initialize()

	rec2 := &LRecord{}
	rec2.wd.Initialize()

	var tid uint64 = 0

	ngo := 10
	locked := make([]int, ngo)
	unlocked := make([]int, ngo)

	var wg sync.WaitGroup
	for i := 0; i < ngo; i++ {
		wg.Add(1)
		go func(id int) {
			//done := time.NewTimer(time.Duration(1 * time.Second)).C
			req := &LockReq{
				id:    id,
				state: make(chan int, 1),
			}

			for {
				localtid := atomic.AddUint64(&tid, 1)
				/*select {
				case <-done:
					break
				default:*/
				if localtid > 1000 {
					break
				}

				req.tid = TID(localtid)

				if id >= 5 {
					success := false
					for !success {
						ok := rec.RLock(req)
						if ok {
							locked[id]++
							if rec.wd.LockState() != LOCK_SH {
								t.Errorf("I rlocked it! %x\n", id)
							}

							if rec.Upgrade(req) {
								if rec.wd.LockState() != LOCK_EX {
									t.Errorf("I upgraded it! %x \n", id)
								}
								ok := rec2.WLock(req)
								if ok {
									if rec2.wd.LockState() != LOCK_EX {
										t.Errorf("I locked it 2! %x \n", id)
									}
									rec2.WUnlock(req)
								} else {
									rec.WUnlock(req)
									unlocked[id]++
									continue
								}
								rec.WUnlock(req)
							} else {
								rec.RUnlock(req)
								unlocked[id]++
								continue
							}

							success = true
							//rw.RUnlock()
							unlocked[id]++
						}
					}
				} else {
					success := false
					for !success {
						ok := rec.WLock(req)
						if ok {
							locked[id]++
							if rec.wd.LockState() != LOCK_EX {
								t.Errorf("I locked it! %x \n", id)
							}

							ok := rec2.RLock(req)
							if ok {
								if rec2.wd.LockState() != LOCK_SH {
									t.Errorf("I rlocked it 2! %x\n", id)
								}
								rec2.RUnlock(req)
							} else {
								rec.WUnlock(req)
								unlocked[id]++
								continue
							}

							rec.WUnlock(req)
							success = true
							unlocked[id]++
						}
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
	fmt.Printf("tid %v, Locked %v\n", tid, total_locked)
	if total_locked != total_unlocked {
		t.Errorf("Mismatched %v %v \n", total_locked, total_unlocked)
	}
}
