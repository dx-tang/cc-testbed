package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var input = flag.String("ti", "ti.conf", "transaction information")
var out = flag.String("to", "tp.out", "output transaction combinaion")

type TXN struct {
	MP   int
	RR   float64
	TLEN int
}

type txnPair struct {
	start int
	end   int
}

type pairData struct {
	points   int
	RRDist   float64
	TLENDist int
}

func main() {
	flag.Parse()
	in, err := os.OpenFile(*input, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("Open Input File Error %v\n", *input)
	}
	defer in.Close()

	reader := bufio.NewReader(in)

	var txnMap map[TXN][]int
	var txnArray []TXN
	var strbytes []byte
	var totalNum int

	strbytes, _, err = reader.ReadLine()
	if err != nil {
		fmt.Printf("Read Line Error %v\n", err.Error())
	}
	totalNum, err = strconv.Atoi(string(strbytes))
	if err != nil {
		fmt.Printf("Parse Integer Error %v\n", string(strbytes))
	}

	txnMap = make(map[TXN][]int)
	txnArray = make([]TXN, totalNum)
	txnArray = txnArray[:0]

	// Parse Input Data
	for i := 0; i < totalNum; i++ {
		one := TXN{}
		strbytes, _, err = reader.ReadLine()
		if err != nil {
			fmt.Printf("Read Line Error %v\n", err.Error())
		}
		valStrs := strings.Split(string(strbytes), "\t")
		one.MP, err = strconv.Atoi(valStrs[0])
		if err != nil {
			fmt.Printf("String To Int Error %v\n", valStrs[0])
		}
		var r, w int
		r, err = strconv.Atoi(valStrs[1])
		if err != nil {
			fmt.Printf("String To Int Error %v\n", valStrs[1])
		}
		w, err = strconv.Atoi(valStrs[2])
		if err != nil {
			fmt.Printf("String To Int Error %v\n", valStrs[2])
		}

		one.TLEN = r + w
		one.RR = float64(r) / float64(r+w)
		txnId, ok := txnMap[one]
		if ok {
			n := len(txnId)
			txnId = txnId[:n+1]
			txnId[n] = i
			txnMap[one] = txnId
		} else {
			n := len(txnArray)
			txnArray = txnArray[:n+1]
			txnArray[n] = one

			txnId = make([]int, totalNum)
			txnId = txnId[:1]
			txnId[0] = i
			txnMap[one] = txnId
		}
	}

	// Generate All Pairs of Transactions
	var maxRRDist, minRRDist float64
	var maxTLENDist, minTLENDist int
	var pairMap map[txnPair]*pairData
	pairMap = make(map[txnPair]*pairData)

	for i := 0; i < len(txnArray); i++ {
		for j := i + 1; j < len(txnArray); j++ {

			if txnArray[i].MP <= 1 && txnArray[j].MP <= 1 {
				continue
			}

			tp := txnPair{
				start: i,
				end:   j,
			}
			pd := &pairData{}
			pd.RRDist = txnArray[i].RR - txnArray[j].RR
			if pd.RRDist < 0 {
				pd.RRDist = -pd.RRDist
			}
			pd.TLENDist = txnArray[i].TLEN - txnArray[j].TLEN
			if pd.TLENDist < 0 {
				pd.TLENDist = -pd.TLENDist
			}
			pairMap[tp] = pd

			if pd.RRDist < minRRDist {
				minRRDist = pd.RRDist
			} else if pd.RRDist > maxRRDist {
				maxRRDist = pd.RRDist
			}

			if pd.TLENDist < minTLENDist {
				minTLENDist = pd.TLENDist
			} else if pd.TLENDist > maxTLENDist {
				maxTLENDist = pd.TLENDist
			}
		}
	}

	o, outErr := os.OpenFile(*out, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if outErr != nil {
		fmt.Printf("Open File %s Error\n", *o)
		return
	}
	defer o.Close()

	for tp, _ := range pairMap {
		pd := pairMap[tp]
		rrper := pd.RRDist / maxRRDist
		tlenper := float64(pd.TLENDist) / float64(maxTLENDist)
		if rrper > 0.85 || tlenper > 0.85 {
			str := formatStr(totalNum, txnArray, txnMap, tp, 0.25)
			o.WriteString(str + "\n")
			str = formatStr(totalNum, txnArray, txnMap, tp, 0.5)
			o.WriteString(str + "\n")
			str = formatStr(totalNum, txnArray, txnMap, tp, 0.75)
			o.WriteString(str + "\n")
		} else if rrper > 0.5 || tlenper > 0.5 {
			str := formatStr(totalNum, txnArray, txnMap, tp, 0.33)
			o.WriteString(str + "\n")
			str = formatStr(totalNum, txnArray, txnMap, tp, 0.67)
			o.WriteString(str + "\n")
		} else if rrper > 0.15 || tlenper > 0.15 {
			str := formatStr(totalNum, txnArray, txnMap, tp, 0.5)
			o.WriteString(str + "\n")
		} else {
			pd.points = 0
		}
	}

	for i := 0; i < totalNum; i++ {
		var str string
		if i == 0 {
			str = "100"
		} else {
			str = "0"
		}
		for j := 1; j < totalNum; j++ {
			if j == i {
				str += ":100"
			} else {
				str += ":0"
			}
		}
		o.WriteString(str + "\n")
		fmt.Printf("%s\n", str)
	}
}

func formatStr(totalNum int, txnArray []TXN, txnMap map[TXN][]int, tp txnPair, combineRate float64) string {
	//fmt.Printf("start %v; end %v; rate %v\n", tp.start, tp.end, combineRate)
	perArray := make([]int, totalNum)

	// Start
	txnId := txnMap[txnArray[tp.start]]
	n := len(txnId)
	unit := int((combineRate / float64(n)) * 100)
	total := int(combineRate * 100)
	for i := 0; i < n-1; i++ {
		perArray[txnId[i]] = int((i + 1) * unit)
	}
	perArray[txnId[n-1]] = total - unit*(n-1)

	// End
	txnId = txnMap[txnArray[tp.end]]
	combineRate = 1 - combineRate
	n = len(txnId)
	unit = int((combineRate / float64(n)) * 100)
	total = int(combineRate * 100)
	for i := 0; i < n-1; i++ {
		perArray[txnId[i]] = int((i + 1) * unit)
	}
	perArray[txnId[n-1]] = total - unit*(n-1)

	var retStr string
	for i := 0; i < len(perArray)-1; i++ {
		retStr += strconv.Itoa(perArray[i]) + ":"
	}
	retStr += strconv.Itoa(perArray[len(perArray)-1])
	fmt.Printf("%s\n", retStr)
	return retStr
}