package raft

import "sync"

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	Set(key string, value string)

	Get(key string) ([]byte, bool)

	// HasData returns true iff any Sets were made on this Storage.
	HasData() bool
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu sync.Mutex
	m  map[string]string
}

func NewMapStorage() *MapStorage {
	m := make(map[string]string)
	return &MapStorage{
		m: m,
	}
}

func (ms *MapStorage) Get(key string) (string, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]
	return v, found
}

func (ms *MapStorage) Set(key string, value string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0
}
