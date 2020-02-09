package storage

import (
	"fmt"
	"sync"
)

// InMemory implements Store, meant to be used in unit tests.
type InMemory struct {
	maxPutErrsPerKey int

	sync.Mutex
	m       map[Key]Value
	putErrs map[Key]int
}

func NewInMemory(maxPutErrsPerKey int) *InMemory {
	return &InMemory{
		maxPutErrsPerKey: maxPutErrsPerKey,
		m:                make(map[Key]Value),
		putErrs:          make(map[Key]int),
	}
}

func (s *InMemory) Get(k Key) (Value, error) {
	s.Lock()
	defer s.Unlock()
	v, ok := s.m[k]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

func (s *InMemory) Put(k Key, v Value) error {
	s.Lock()
	defer s.Unlock()
	if count := s.putErrs[k]; count < s.maxPutErrsPerKey {
		s.putErrs[k] = count + 1
		return fmt.Errorf("error %d on put of %v", 1+count, k)
	}
	s.putErrs[k] = 0
	s.m[k] = v
	return nil
}

func (s *InMemory) Delete(k Key) error {
	s.Lock()
	defer s.Unlock()
	delete(s.m, k)
	return nil
}
