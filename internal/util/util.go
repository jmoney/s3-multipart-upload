package util

import (
	"log"
	"os"
)

var (
	elog *log.Logger = log.New(os.Stderr, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
)

// Function that take a tuple of a value and error.  If error is not nil, it exits and logs. Else, returns the value.
func Must[T any](value T, err error) T {
	if err != nil {
		elog.Panic(err)
	}
	return value
}
