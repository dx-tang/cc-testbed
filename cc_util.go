package testbed

import (
	crand "crypto/rand"
)

//Generate random numbers within [0, n)
func RandN(seed *uint32, n uint32) uint32 {
	if n == 0 {
		//panic("n is 0")
		return 0
	}
	*seed = *seed*1664525 + 1013904223
	if n<<3 == 0 {
		n = 1
	}
	return ((*seed & 0x7fffffff) % (n << 3)) >> 3
}

func Randstr(sz int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, sz)
	crand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}
