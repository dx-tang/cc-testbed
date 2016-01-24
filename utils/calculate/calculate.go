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
var out = flag.String("out", "", "Output File to Calculate")
var period = flag.Int("p", 3, "Times for each test")
var t = flag.String("t", "int", "Type to Compute")

func main() {
	flag.Parse()

	in, inErr := os.OpenFile(*input, os.O_RDONLY, 0666)
	if inErr != nil {
		fmt.Printf("Open File %s Error\n", *input)
		return
	}
	defer in.Close()

	reader := bufio.NewReader(in)

	o, outErr := os.OpenFile(*out, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if outErr != nil {
		fmt.Printf("Open File %s Error\n", *o)
		return
	}
	defer o.Close()

	var isum int
	var fsum float64
	var itmp int
	var ftmp float64

	var intErr error

	for {
		isum = 0
		fsum = 0.0
		for i := 0; i < *period; i++ {
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
			if strings.Compare(*t, "int") == 0 {
				itmp, intErr = strconv.Atoi(line)
				if intErr != nil {
					fmt.Printf("String %s to Int Error\n", line)
					return
				}
				isum += itmp
			} else if strings.Compare(*t, "float") == 0 {
				ftmp, intErr = strconv.ParseFloat(line, 64)
				if intErr != nil {
					fmt.Printf("String %s to Int Error\n", line)
					return
				}
				fsum += ftmp
			} else {
				fmt.Printf("Type %v not Supported\n", *t)
				return
			}
		}

		if strings.Compare(*t, "int") == 0 {
			o.WriteString(fmt.Sprintf("%v\n", isum/(*period)))
		} else if strings.Compare(*t, "float") == 0 {
			o.WriteString(fmt.Sprintf("%.5f\n", fsum/float64(*period)))
		}

	}
}
