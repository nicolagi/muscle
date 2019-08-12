package storage

import (
	"github.com/stretchr/testify/mock"
)

type StoreMock struct {
	mock.Mock
}

func (s *StoreMock) Contains(Key) (bool, error) {
	panic("implement me")
}

func (s *StoreMock) Get(k Key) (Value, error) {
	arguments := s.Called(k)
	var ok bool
	b, ok := arguments.Get(0).(Value)
	if !ok {
		b = nil
	}
	return b, arguments.Error(1)
}

func (s *StoreMock) Put(k Key, v Value) error {
	return s.Called(k, v).Error(0)
}

func (s *StoreMock) Delete(k Key) error {
	return s.Called(k).Error(0)
}

func (s *StoreMock) ForEach(fn func(Key) error) error {
	return s.Called(fn).Error(0)
}
