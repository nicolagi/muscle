package main

import "fmt"

func errorv(typeMethod string, err error) error {
	return fmt.Errorf("github.com/nicolagi/muscle/cmd/musclefs."+typeMethod+": %v", err)
}

func errorf(typeMethod, format string, a ...interface{}) error {
	return fmt.Errorf("github.com/nicolagi/muscle/cmd/musclefs."+typeMethod+": "+format, a...)
}
