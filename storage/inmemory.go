package storage

import "sync"

// InMemory implements Store, meant to be used in unit tests.
type InMemory struct {
	sync.Mutex
	m map[Key]Value
}

func NewInMemory() *InMemory {
	return &InMemory{m: make(map[Key]Value)}
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
	s.m[k] = v
	return nil
}

func (s *InMemory) Delete(k Key) error {
	s.Lock()
	defer s.Unlock()
	delete(s.m, k)
	return nil
}
