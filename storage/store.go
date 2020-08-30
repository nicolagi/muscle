package storage

import (
	"errors"
	"fmt"

	"github.com/nicolagi/muscle/config"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrNotImplemented = errors.New("not implemented")
)

type Key string

type Value []byte

type Store interface {
	Get(Key) (Value, error)
	Put(Key, Value) error
	Delete(Key) error
}

type Lister interface {
	// TODO: This interface is strange; how can the error be known right away, but the
	// keys are progressively written to the channel? Isn't it possible to encounter an error
	// after List() returns, e.g., paginating results sets?
	List() (keys chan string, err error)
}

type Enumerable interface {
	Store
	// TODO: "Contains" does not pertain to an Enumerable entity. Also, can we prevent embedding the Store?
	Contains(Key) (bool, error)
	ForEach(func(Key) error) error
}

func NewStore(c *config.C) (Store, error) {
	switch c.Storage {
	case "disk":
		return NewDiskStore(c.DiskStoreDir), nil
	case "null":
		return NullStore{}, nil
	case "s3":
		return newS3Store(c)
	default:
		return nil, fmt.Errorf("%q: %w", c.Storage, ErrNotImplemented)
	}
}
