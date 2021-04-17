package config

import "fmt"

func errorf(typeMethod, format string, a ...interface{}) error {
	return fmt.Errorf("github.com/nicolagi/muscle/internal/config."+typeMethod+": "+format, a...)
}
