package main

import "fmt"

func errorf(typeMethod, format string, a ...interface{}) error {
	return fmt.Errorf("github.com/nicolagi/muscle/cmd/muscle."+typeMethod+": "+format, a...)
}
