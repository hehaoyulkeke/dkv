package raft

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	Set(key string, value string)

	Get(key string) (string, bool)

}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	m  map[string]string
}

func NewMapStorage() *MapStorage {
	m := make(map[string]string)
	return &MapStorage{
		m: m,
	}
}

func (ms *MapStorage) Get(key string) (string, bool) {
	v, found := ms.m[key]
	return v, found
}

func (ms *MapStorage) Set(key string, value string) {
	ms.m[key] = value
}

