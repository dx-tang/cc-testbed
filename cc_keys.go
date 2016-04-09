package testbed

import (
	//"runtime/debug"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	ONEKEYWIDTH  = 8
	KEYLENTH     = 4
	KEYLENTHBYTE = 32
)

type Key [KEYLENTHBYTE]byte

//type int64 int64

// Composite Keys; Not Used Yet
type CompKey struct {
	keysArray []int64
}

func CKey(x []int64) Key {
	var k Key
	var i, j uint64
	for j = 0; j < uint64(len(x)); j++ {
		for i = 0; i < ONEKEYWIDTH; i++ {
			k[j*ONEKEYWIDTH+i] = byte(uint64(x[j]) >> (i * 8))
		}
	}
	return k
}

// Update an existing key
func UKey(x [KEYLENTH]int64, k *Key) {
	var i, j uint64
	for j = 0; j < KEYLENTH; j++ {
		for i = 0; i < ONEKEYWIDTH; i++ {
			(*k)[j*ONEKEYWIDTH+i] = byte(uint64(x[j]) >> (i * 8))
		}
	}
}

func ParseKey(key Key, index int) int64 {
	if index >= KEYLENTH {
		clog.Error("Index %v out of range for key length %v\n", index, KEYLENTH)
	}

	var onekey int64
	/*
		for i := uint(0); i < ONEKEYWIDTH; i++ {
			onekey += int64(key[index*ONEKEYWIDTH+int(i)]) << (i * 8)
		}*/
	for i := ONEKEYWIDTH - 1; i >= 0; i-- {
		onekey += int64(key[index*ONEKEYWIDTH+i]) << (uint(i) * 8)
	}

	return int64(onekey)
}
