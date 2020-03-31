package sladder

import (
	"io/ioutil"
	"log"
)

var (
	discardLogger = log.New(ioutil.Discard, "", 0)
)

// Logger is logging abstraction.
type Logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})

	Print(v ...interface{})
	Printf(format string, v ...interface{})

	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
}