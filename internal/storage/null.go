package storage

type NullStore struct{}

func (NullStore) Get(Key) (Value, error) {
	return nil, ErrNotFound
}

func (NullStore) Put(Key, Value) error {
	return nil
}

func (NullStore) Delete(Key) error {
	return nil
}

func (NullStore) Contains(Key) (bool, error) {
	return false, nil
}

func (NullStore) ForEach(func(Key) error) error {
	return nil
}
