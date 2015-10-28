package clog

import (
	"io"
	"log"
	"os"
)

var (
	info  *log.Logger
	err   *log.Logger
	debug *log.Logger
)

func init() {
	info = log.New(os.Stdout, "Info: ", log.Ldate|log.Ltime|log.Lmicroseconds)
	err = log.New(os.Stderr, "Error: ", log.Ldate|log.Ltime|log.Lmicroseconds)
	debug = nil
}

func SetDebugOutput(w io.Writer) {
	debug = log.New(w, "Debug: ", log.Ldate|log.Ltime|log.Lmicroseconds)
}

func Info(s string, v ...interface{}) {
	info.Printf(s, v...)
}

func Error(s string, v ...interface{}) {
	err.Fatalf(s, v...)
}

func Debug(s string, v ...interface{}) {
	if debug != nil {
		debug.Printf(s, v...)
	}
}
