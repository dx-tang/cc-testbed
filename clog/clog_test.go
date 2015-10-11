package clog

import (
	"testing"
)

func TestLog(t *testing.T) {
	Info("This is for info test")

	Debug("This won't show")
	*d = true
	Debug("This would show for Debug")

	//Error("Error Happen")
}
