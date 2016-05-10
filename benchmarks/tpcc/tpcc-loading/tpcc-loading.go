package main

import (
	"flag"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/totemtang/cc-testbed"
	"github.com/totemtang/cc-testbed/clog"
)

var wl = flag.String("wl", "../tpcc.txt", "workload to be used")
var loaders = flag.Int("l", 2, "How many loaders for data loading")
var dataDir = flag.String("dd", "../data", "TPCC Data Dir")
var prof = flag.Bool("prof", false, "Whether Profile")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart
	testbed.WLTYPE = testbed.TPCCWL

	nParts := nWorkers

	nLoaders := *loaders
	clog.Info("Number of Loaders %v \n", nLoaders)

	start := time.Now()
	tpccWL := testbed.NewTPCCWL(*wl, nParts, false, nWorkers, 1, "100:0:0:0:0:0", 0, 0, *dataDir)
	store := tpccWL.GetStore()
	clog.Info("Making Store %3.fs", time.Since(start).Seconds())

	clog.Info("Done with Populating Store\n")

	if *prof {
		f, err := os.Create("tpcc-loading.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	mode := testbed.OCC

	start = time.Now()

	tables := store.GetTables()
	newTables := make([]testbed.Table, len(tables))
	for i := 0; i < len(tables); i++ {
		if i == testbed.NEWORDER {
			start := time.Now()
			newTables[i] = testbed.MakeNewOrderTable(*testbed.NumPart, true, mode)
			clog.Info("Making NewOrder %.2f", time.Since(start).Seconds())
		} else if i == testbed.ORDER {
			start := time.Now()
			newTables[i] = testbed.MakeOrderTable(nParts, *testbed.NumPart, true, mode)
			clog.Info("Making Order %.2f", time.Since(start).Seconds())
		} else if i == testbed.CUSTOMER {
			start := time.Now()
			newTables[i] = testbed.MakeCustomerTable(nParts, *testbed.NumPart, true, mode)
			clog.Info("Making Customer %.2f", time.Since(start).Seconds())
		} else if i == testbed.HISTORY {
			start := time.Now()
			newTables[i] = tables[i]
			clog.Info("Making History %.2f", time.Since(start).Seconds())
		} else if i == testbed.ORDERLINE {
			start := time.Now()
			newTables[i] = testbed.MakeOrderLineTable(nParts, *testbed.NumPart, true, mode)
			clog.Info("Making OrderLine %.2f", time.Since(start).Seconds())
		} else if i == testbed.ITEM {
			start := time.Now()
			newTables[i] = tables[i]
			clog.Info("Making ITEM %.2f", time.Since(start).Seconds())
		} else {
			start := time.Now()
			newTables[i] = testbed.NewBasicTable(nil, nParts, true, mode, i)
			clog.Info("Making BasicTable %.2f", time.Since(start).Seconds())
		}
	}

	clog.Info("Parallel Making Table %.3fs", time.Since(start).Seconds())

	perLoader := nParts / nLoaders
	residue := nParts % nLoaders
	ia := make([][]testbed.IndexAlloc, nLoaders)
	for i := 0; i < nLoaders; i++ {
		ia[i] = make([]testbed.IndexAlloc, len(tables))
		ia[i][testbed.NEWORDER] = &testbed.NewOrderIndexAlloc{}
		ia[i][testbed.NEWORDER].OneAllocate()
		ia[i][testbed.ORDER] = &testbed.OrderIndexAlloc{}
		ia[i][testbed.ORDER].OneAllocate()
		ia[i][testbed.ORDERLINE] = &testbed.OrderLineIndexAlloc{}
		ia[i][testbed.ORDERLINE].OneAllocate()
	}

	start = time.Now()

	var wg sync.WaitGroup
	for i := 0; i < nLoaders; i++ {
		wg.Add(1)
		go func(n int) {
			iaAR := ia[n]
			begin := n * perLoader
			if n < residue {
				begin += n
			} else {
				begin += residue
			}
			end := begin + perLoader
			if n < residue {
				end++
			}
			clog.Info("Loading start from %v to %v", begin, end)
			for j := 0; j < len(tables); j++ {
				if j != testbed.ITEM && j != testbed.HISTORY {
					tables[j].BulkLoad(newTables[j], iaAR[j], begin, end)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	clog.Info("Loading Data Takes %.3fs", time.Since(start).Seconds())

	var k testbed.Key
	k[0] = 1
	k[1] = 0
	k[2] = 1
	_, err := newTables[testbed.ORDER].GetRecByID(k, 1)
	if err != nil {
		clog.Error("Get Record Error %v\n", err)
	}

}
