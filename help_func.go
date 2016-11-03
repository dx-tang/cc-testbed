package testbed

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/totemtang/cc-testbed/clog"
)

type WorkerConfig struct {
	padding1 [PADDING]byte
	ID       int
	protocol int64
	start    int64
	end      int64
	padding2 [PADDING]byte
}

func BuildWorkerConfig(f string) []WorkerConfig {
	wc := make([]WorkerConfig, *NumPart)

	tf, err := os.OpenFile(f, os.O_RDONLY, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer tf.Close()

	reader := bufio.NewReader(tf)

	var data []byte
	var splits []string

	_, _, err = reader.ReadLine()
	if err != nil {
		clog.Error("Read Header Error %v", err.Error())
	}

	for i := 0; i < *NumPart; i++ {
		data, _, err = reader.ReadLine()
		if err != nil {
			clog.Error("Read Line %v Error %v", i, err.Error())
		}
		wc[i].ID = i
		splits = strings.Split(string(data), "\t")
		wc[i].start, _ = strconv.ParseInt(splits[1], 10, 32)
		wc[i].end, _ = strconv.ParseInt(splits[2], 10, 32)
		wc[i].protocol, _ = strconv.ParseInt(splits[3], 10, 32)
	}

	return wc
}

func BuildUseLatch(wc []WorkerConfig) []bool {
	useLatch := make([]bool, len(wc))
	for i := 0; i < len(wc); i++ {
		if *SysType == ADAPTIVE && !*Hybrid {
			if wc[i].protocol == PARTITION {
				useLatch[i] = false
			} else {
				useLatch[i] = true
			}
		} else if *SysType == PARTITION {
			useLatch[i] = false
		} else {
			useLatch[i] = true
		}
	}
	return useLatch
}
