package mixlock

import (
	"fmt"
	"sync"
	"testing"
)

func TestMixLock(t *testing.T) {
	s := new(MixLock)
	s.Lock()
	s.Unlock(0)

	s.RLock()
	s.RUnlock()

	y := 0
	x := 0
	n := 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			for j := 0; j < 10; j++ {
				for {
					successs, _ := s.Lock()
					if successs {
						break
					}
				}
				if x+y != 0 {
					t.Fatal("Wrong")
				}
				y = y + 1
				x = x - 1
				s.Unlock(0)
			}
			wg.Done()
		}(i)
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				for {
					successs, _ := s.RLock()
					if successs {
						break
					}
				}
				if x+y != 0 {
					t.Fatalf("Bad read %v %v\n", x, y)
				}
				s.RUnlock()
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				for {
					successs, _ := s.RLock()
					if successs {
						break
					}
				}
				if x+y != 0 {
					t.Fatalf("Bad read %v %v\n", x, y)
				}
				fmt.Printf("Read %v\n", s.CheckLock())
				if s.Upgrade() {
					y = y + 1
					x = x - 1
					fmt.Printf("Check %v\n", s.CheckLock())
					s.Unlock(0)
				}
				//s.RUnlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if x+y != 0 {
		t.Fatalf("Bad lock\n")
	}
	fmt.Printf("Passed TestNoWaitLock\n")
}
