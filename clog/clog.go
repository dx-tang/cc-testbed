package clog

import (
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

func OpenDebug() {
	debug = log.New(os.Stdout, "Debug: ", log.Ldate|log.Ltime|log.Lmicroseconds)
}

func CloseDebug() {
	debug = nil
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
