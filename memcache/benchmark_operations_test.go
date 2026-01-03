/*
Copyright 2011 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"fmt"
	"net"
	"testing"
)

// setupBenchmarkClient creates a client for benchmarking
// Requires memcached running on localhost:11211
func setupBenchmarkClient(b *testing.B) *Client {
	c, err := net.Dial("tcp", "localhost:11211")
	if err != nil {
		b.Skipf("skipping benchmark; no server running at localhost:11211")
	}
	c.Close()

	client, err := New("localhost:11211")
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}

	return client
}

// BenchmarkSetGet benchmarks Set and Get operations
func BenchmarkSetGet(b *testing.B) {
	c := setupBenchmarkClient(b)
	key := []byte("benchmark_key")
	value := []byte("benchmark value")

	b.ResetTimer()
	item := &Item{Key: key, Value: value}
	itemGet := new(Item)
	for i := 0; i < b.N; i++ {
		err := c.Set(item)
		if err != nil {
			b.Fatal(err)
		}
		err = c.GetWithItem(key, itemGet)
		if err != nil {
			b.Fatal(err)
		}
	}
}


// BenchmarkSet benchmarks Set operations only
func BenchmarkSet(b *testing.B) {
	c := setupBenchmarkClient(b)
	value := []byte("benchmark value for set operation")

	b.ResetTimer()
	key := []byte("bench_key")
	item := &Item{Key: key, Value: value}

	for i := 0; i < b.N; i++ {
		err := c.Set(item)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet benchmarks Get operations only
func BenchmarkGet(b *testing.B) {
	c := setupBenchmarkClient(b)
	// Pre-populate with data
	key := []byte("bench_get_key")
	value := []byte("benchmark value for get operation")
	err := c.Set(&Item{Key: []byte(key), Value: value})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	itemGet := new(Item)
	for i := 0; i < b.N; i++ {
		err := c.GetWithItem(key, itemGet)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetMulti benchmarks GetMulti operations

// BenchmarkAdd benchmarks Add operations
func BenchmarkAdd(b *testing.B) {
	c := setupBenchmarkClient(b)
	value := []byte("benchmark value for add operation")

	// Pre-delete keys to ensure Add will succeed
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_add_key_%d", i)
		c.Delete([]byte(key)) // Ignore error if key doesn't exist
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_add_key_%d", i)
		err := c.Add(&Item{Key: []byte(key), Value: value})
		if err != nil && err != ErrNotStored {
			b.Fatal(err)
		}
	}
}

// BenchmarkSetLargeValue benchmarks Set with large values (100KB)
func BenchmarkSetLargeValue(b *testing.B) {
	c := setupBenchmarkClient(b)
	value := make([]byte, 100*1024) // 100KB (memcached default max is 1MB)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	item := &Item{
		Key:   []byte("bench_large_key"),
		Value: value,
	}
	for i := 0; i < b.N; i++ {
		err := c.Set(item)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetLargeValue benchmarks Get with large values (100KB)
func BenchmarkGetLargeValue(b *testing.B) {
	c := setupBenchmarkClient(b)
	value := make([]byte, 100*1024) // 100KB
	for i := range value {
		value[i] = byte(i % 256)
	}
	key := []byte("bench_large_get_key")
	err := c.Set(&Item{Key: []byte(key), Value: value})
	if err != nil {
		b.Fatal(err)
	}
	getItem := &Item{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.GetWithItem(key, getItem)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParallelSet benchmarks parallel Set operations
func BenchmarkParallelSet(b *testing.B) {
	c := setupBenchmarkClient(b)
	value := []byte("parallel benchmark value")

	b.ResetTimer()
	item := &Item{
		Value: value,
		Key:   []byte("bench_parallel_key"),
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			err := c.Set(item)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkParallelGet benchmarks parallel Get operations
func BenchmarkParallelGet(b *testing.B) {
	c := setupBenchmarkClient(b)
	// Pre-populate with data
	numKeys := 100
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bench_parallel_get_key_%d", i)
		keys[i] = []byte(key)
		err := c.Set(&Item{Key: []byte(key), Value: []byte(fmt.Sprintf("value_%d", i))})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%numKeys]
			_, err := c.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
