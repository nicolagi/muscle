package storage

import "fmt"

func errorf(typeMethod, format string, a ...interface{}) error {
	return fmt.Errorf("github.com/nicolagi/muscle/internal/storage."+typeMethod+": "+format, a...)
}
