package testbed

const (
	ONEKEYWIDTH  = 4
	KEYLENTH     = 4
	KEYLENTHBYTE = 16
)

//type Key [KEYLENTHBYTE]byte
type Key [KEYLENTH]int

//type int64 int64

// Composite Keys; Not Used Yet
type CompKey struct {
	keysArray []int
}

/*
func CKey(x []int) Key {
	var k Key
	var i, j uint
	for j = 0; j < uint(len(x)); j++ {
		for i = 0; i < ONEKEYWIDTH; i++ {
			k[j*ONEKEYWIDTH+i] = byte(uint(x[j]) >> (i * 8))
		}
	}
	return k
}

// Update an existing key
func UKey(x [KEYLENTH]int, k *Key) {
	var i, j uint
	for j = 0; j < KEYLENTH; j++ {
		for i = 0; i < ONEKEYWIDTH; i++ {
			(*k)[j*ONEKEYWIDTH+i] = byte(uint(x[j]) >> (i * 8))
		}
	}
}

func ParseKey(key Key, index int) int {
	if index >= KEYLENTH {
		clog.Error("Index %v out of range for key length %v\n", index, KEYLENTH)
	}

	var onekey int

	for i := ONEKEYWIDTH - 1; i >= 0; i-- {
		onekey += int(key[index*ONEKEYWIDTH+i]) << (uint(i) * 8)
	}

	return int(onekey)
}*/
