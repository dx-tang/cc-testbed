package main

import (
	"flag"
	"os"
	"runtime"
	"runtime/pprof"
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
	isPartition := true
	nLoaders := *loaders
	clog.Info("Number of Loaders %v \n", nLoaders)

	store := testbed.NewStore(*wl, nParts, isPartition)

	clog.Info("Done with Populating Store\n")

	if *prof {
		f, err := os.Create("tpcc-loading.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	mode := testbed.PARTITION

	start := time.Now()

	tables := store.GetTables()
	newTables := make([]testbed.Table, len(tables))
	for i := 0; i < len(tables); i++ {
		if i == testbed.NEWORDER {
			start := time.Now()
			newTables[i] = testbed.MakeNewOrderTablePara(*testbed.NumPart, isPartition, mode, nLoaders)
			clog.Info("Making NewOrder %.2f", time.Since(start).Seconds())
		} else if i == testbed.ORDER {
			start := time.Now()
			newTables[i] = testbed.MakeOrderTablePara(nParts, *testbed.NumPart, isPartition, mode, nLoaders)
			clog.Info("Making Order %.2f", time.Since(start).Seconds())
		} else if i == testbed.CUSTOMER {
			start := time.Now()
			newTables[i] = testbed.MakeCustomerTablePara(nParts, *testbed.NumPart, isPartition, mode, nLoaders)
			clog.Info("Making Customer %.2f", time.Since(start).Seconds())
		} else if i == testbed.HISTORY {
			start := time.Now()
			newTables[i] = testbed.MakeHistoryTable(nParts, *testbed.NumPart, isPartition, mode)
			clog.Info("Making History %.2f", time.Since(start).Seconds())
		} else if i == testbed.ORDERLINE {
			start := time.Now()
			newTables[i] = testbed.MakeOrderLineTablePara(nParts, *testbed.NumPart, isPartition, mode, nLoaders)
			clog.Info("Making OrderLine %.2f", time.Since(start).Seconds())
		} else if i == testbed.ITEM {
			start := time.Now()
			newTables[i] = testbed.NewBasicTable(nil, 1, false, mode, testbed.ITEM)
			clog.Info("Making ITEM %.2f", time.Since(start).Seconds())
		} else {
			start := time.Now()
			newTables[i] = testbed.NewBasicTablePara(nil, nParts, isPartition, mode, i, nLoaders)
			clog.Info("Making BasicTable %.2f", time.Since(start).Seconds())
		}
	}

	clog.Info("Parallel Making Table %.3f", time.Since(start).Seconds())

}
