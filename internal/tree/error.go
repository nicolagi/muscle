package tree

import "fmt"

var (
	ErrExist      = fmt.Errorf("exists")
	ErrNotEmpty   = fmt.Errorf("not empty")
	ErrNotExist   = fmt.Errorf("does not exist")
	ErrPermission = fmt.Errorf("permission denied")
	ErrReadOnly   = fmt.Errorf("read-only")
)

func errorv(typeMethod string, err error) error {
	return fmt.Errorf("github.com/nicolagi/muscle/internal/tree."+typeMethod+": %v", err)
}

func errorf(typeMethod, format string, a ...interface{}) error {
	return fmt.Errorf("github.com/nicolagi/muscle/internal/tree."+typeMethod+": "+format, a...)
}
