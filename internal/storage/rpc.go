package storage

import (
	"net/rpc"
	"strings"
)

type GetArgs struct {
	Key Key
}

type GetReply struct {
	Value []byte
}

type PutArgs struct {
	Key   Key
	Value Value
}

type PutReply struct{}

type DeleteArgs struct {
	Key Key
}

type DeleteReply struct{}

// StoreService wraps a Store implementation for use in a net/rpc client-server application.
type StoreService struct {
	delegate Store
}

func NewStoreService(delegate Store) *StoreService {
	return &StoreService{delegate: delegate}
}

func (s *StoreService) Get(args GetArgs, reply *GetReply) error {
	value, err := s.delegate.Get(args.Key)
	if err != nil {
		return err
	}
	reply.Value = value
	return nil
}

func (s *StoreService) Put(args PutArgs, reply *PutReply) error {
	return s.delegate.Put(args.Key, args.Value)
}

func (s *StoreService) Delete(args DeleteArgs, reply *DeleteReply) error {
	return s.delegate.Delete(args.Key)
}

// RemoteStore implements Store by calling a remote endpoint serving a StoreService using net/rpc.
type RemoteStore struct {
	client *rpc.Client
}

func NewRemoteStore(network, address string) (*RemoteStore, error) {
	client, err := rpc.DialHTTP(network, address)
	if err != nil {
		return nil, err
	}
	return &RemoteStore{client: client}, nil
}

func (s *RemoteStore) Get(key Key) (Value, error) {
	var reply GetReply
	err := s.client.Call("StoreService.Get", GetArgs{Key: key}, &reply)
	if err != nil {
		if strings.HasSuffix(err.Error(), "not found") {
			err = ErrNotFound
		}
		return nil, err
	}
	return reply.Value, nil
}

func (s *RemoteStore) Put(key Key, value Value) error {
	return s.client.Call("StoreService.Put", PutArgs{Key: key, Value: value}, nil)
}

func (s *RemoteStore) Delete(key Key) error {
	return s.client.Call("StoreService.Delete", DeleteArgs{Key: key}, nil)
}
