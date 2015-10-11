package clog

import (
	"flag"
	"io"
	"log"
	"os"
)

var d = flag.Bool("d", false, "Indicate whether output debug info")

var (
	info  *log.Logger
	err   *log.Logger
	debug *log.Logger
)

func init() {
	info = log.New(os.Stdout, "Info: ", log.Ldate|log.Ltime|log.Lmicroseconds)
	err = log.New(os.Stderr, "Error: ", log.Ldate|log.Ltime|log.Lmicroseconds)
	debug = log.New(os.Stdout, "Debug: ", log.Ldate|log.Ltime|log.Lmicroseconds)
}

func setOutput(w io.Writer) {
	info.SetOutput(w)
	err.SetOutput(w)
	debug.SetOutput(w)
}

func Info(s string, v ...interface{}) {
	info.Printf(s, v...)
}

func Error(s string, v ...interface{}) {
	err.Fatalf(s, v...)
}

func Debug(s string, v ...interface{}) {
	if *d {
		debug.Printf(s, v...)
	}
}
