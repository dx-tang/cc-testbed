package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var input = flag.String("input", "", "Input File to Calculate")
var out = flag.String("out", "", "Output Files to Calculate out1:out2:out3")
var period = flag.Int("p", 3, "Times for each test")

func main() {
	flag.Parse()

	in, inErr := os.OpenFile(*input, os.O_RDONLY, 0666)
	if inErr != nil {
		fmt.Printf("Open File %s Error\n", *input)
		return
	}
	defer in.Close()

	reader := bufio.NewReader(in)

	if *out == "" {
		fmt.Printf("No Output Files\n")
		return
	}

	outStrs := strings.Split(*out, ":")
	outFiles := make([]*os.File, len(outStrs))

	var outErr error
	for i := 0; i < len(outStrs); i++ {
		outFiles[i], outErr = os.OpenFile(outStrs[i], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if outErr != nil {
			fmt.Printf("Open File %s Error\n", outStrs[i])
			return
		}
		defer outFiles[i].Close()
	}

	sum := make([]float64, len(outStrs))

	var strSlice []string

	for {
		for i := 0; i < *period; i++ {
			for j := 0; j < len(outStrs); j++ {
				bytes, strErr := reader.ReadBytes('\n')
				if strErr != nil {
					fmt.Printf("End of File\n")
					return
				}
				var line string
				if bytes[len(bytes)-1] == '\n' {
					line = string(bytes[0 : len(bytes)-1])
				} else {
					line = string(bytes)
				}
				strSlice = strings.Split(line, "\t")
				if len(strSlice) != 2 {
					fmt.Printf("Error Format\n")
					return
				}
				tmp, fErr := strconv.ParseFloat(strSlice[1], 64)
				if fErr != nil {
					fmt.Printf("String %s to Float Error\n", strSlice[1])
					return
				}
				sum[j] += tmp
			}
		}

		for i := 0; i < len(outStrs); i++ {
			outFiles[i].WriteString(fmt.Sprintf("%s \t %.f\n", strSlice[0], sum[i]/float64(*period)))
			sum[i] = 0.0
		}
	}
}
