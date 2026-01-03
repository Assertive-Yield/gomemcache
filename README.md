# gomemcache

[![Go Reference](https://pkg.go.dev/badge/github.com/Assertive-Yield/gomemcache/memcache.svg)](https://pkg.go.dev/github.com/Assertive-Yield/gomemcache/memcache)

A high-performance [memcached](https://memcached.org/) client library for [Go](https://go.dev/).

## Features

- **Production Ready**: Battle-tested and used in production environments
- **Full Protocol Support**: Implements the complete memcache protocol
- **Connection Pooling**: Efficient connection management with configurable pooling
- **Cluster Support**: Automatic sharding across multiple memcached servers
- **Auto-Discovery**: Support for AWS ElastiCache, GCP Memorystore and other cluster configurations
- **High Performance**: Optimized for speed with minimal allocations

## Installation

```shell
go get github.com/Assertive-Yield/gomemcache/memcache
```

## Quick Start

```go
import (
    "fmt"
    "github.com/Assertive-Yield/gomemcache/memcache"
)

func main() {
    // Connect to multiple memcached servers
    mc, err := memcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
    if err != nil {
        panic(err)
    }
    
    // Set an item
    err = mc.Set(&memcache.Item{
        Key:   []byte("foo"),
        Value: []byte("my value"),
        Expiration: 3600, // seconds
    })
    if err != nil {
        panic(err)
    }
    
    // Get an item
    item, err := mc.Get([]byte("foo"))
    if err != nil {
        panic(err)
    }
    fmt.Printf("Value: %s\n", item.Value)
}
```

## Usage Examples

### Basic Operations

```go
// Add (only store if key doesn't exist)
err := mc.Add(&memcache.Item{Key: []byte("key"), Value: []byte("value")})

// Replace (only store if key exists)
err := mc.Replace(&memcache.Item{Key: []byte("key"), Value: []byte("new value")})

// Delete a key
err := mc.Delete([]byte("key"))

// Get multiple keys at once
items, err := mc.GetMulti([][]byte{[]byte("key1"), []byte("key2"), []byte("key3")})

// Touch (update expiration time without fetching)
err := mc.Touch([]byte("key"), 3600)

// Flush all items
err := mc.FlushAll()
```

### Increment/Decrement

```go
// Increment a counter
newValue, err := mc.Increment([]byte("counter"), 1)

// Decrement a counter
newValue, err := mc.Decrement([]byte("counter"), 1)
```

### Compare-And-Swap (CAS)

```go
// Get item with CAS ID
item, err := mc.Get([]byte("key"))
if err != nil {
    panic(err)
}

// Modify and update only if unchanged
item.Value = []byte("new value")
err = mc.CompareAndSwap(item)
if err == memcache.ErrCASConflict {
    // Item was modified by another client
}
```

### Configuration

```go
mc, err := memcache.New("localhost:11211")
if err != nil {
    panic(err)
}

// Set timeout for network operations
mc.Timeout = 100 * time.Millisecond

// Set maximum idle connections per server
mc.MaxIdleConns = 10
```

### Error Handling

```go
item, err := mc.Get([]byte("key"))
if err == memcache.ErrCacheMiss {
    // Key doesn't exist
} else if err != nil {
    // Other error
}
```

### Zero-Allocation Gets with sync.Pool

For high-performance applications, use `GetWithItem` with `sync.Pool` to avoid allocations:

```go
import (
    "sync"
    "github.com/Assertive-Yield/gomemcache/memcache"
)

// Create a pool of reusable Item instances
var itemPool = sync.Pool{
    New: func() interface{} {
        return &memcache.Item{}
    },
}

func getKey(mc *memcache.Client, key []byte) ([]byte, error) {
    // Get an item from the pool
    item := itemPool.Get().(*memcache.Item)
    defer func() {
        // Reset and return to pool
        item.Reset()
        itemPool.Put(item)
    }()
    
    // Populate the item from memcache
    err := mc.GetWithItem(key, item)
    if err != nil {
        return nil, err
    }
    
    // Make a copy of the value before returning the item to the pool
    value := make([]byte, len(item.Value))
    copy(value, item.Value)
    
    return value, nil
}
```

This approach is significantly more efficient than `Get()` for high-throughput scenarios as it eliminates Item allocations.

## Cloud Auto-Discovery

### AWS ElastiCache and GCP Memorystore

Both AWS ElastiCache and GCP Memorystore support auto-discovery using the same `config get cluster` command. The `NewDiscoveryClient` works with both platforms:

```go
import (
    "time"
    "github.com/Assertive-Yield/gomemcache/memcache"
)

// AWS ElastiCache configuration endpoint example
mc, err := memcache.NewDiscoveryClient(
    "my-cluster.cfg.use1.cache.amazonaws.com:11211",
    60 * time.Second, // polling interval (minimum 1 second)
)

// GCP Memorystore discovery endpoint example
// mc, err := memcache.NewDiscoveryClient(
//     "10.0.0.1:11211", // Your GCP Memorystore discovery endpoint
//     60 * time.Second,
// )

if err != nil {
    panic(err)
}

// Stop polling when done (optional, prevents goroutine leak)
defer mc.StopPolling()
```

**How it works:**
- The client sends `config get cluster` to the discovery endpoint
- It receives the list of cluster nodes and configuration version
- Automatically updates the server list when cluster topology changes
- Polls periodically at the specified interval (minimum 1 second)

**Note:** For GCP Memorystore, ensure you're using an instance that supports the `config get` command (typically available in instances with auto-discovery enabled).

## API Documentation

Full API documentation is available at:
https://pkg.go.dev/github.com/Assertive-Yield/gomemcache/memcache

## Benchmarks

To run benchmarks, first start a local memcached instance:

```shell
# Linux/macOS
memcached -d -m 64 -p 11211

# Or using Docker
docker run -d -p 11211:11211 memcached:latest
```

Run all benchmarks:

```shell
go test -bench=. -benchmem -benchtime=3s ./memcache/ | tee benchmark_results.txt
```

See [memcache/benchmark_operations_test.go](memcache/benchmark_operations_test.go) for benchmark implementations.

## Testing

```shell
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run tests with race detector
go test -race ./...
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Credits

Originally created by [Brad Fitzpatrick](https://github.com/bradfitz).

This fork is maintained and refactored by [Assertive Yield](https://github.com/Assertive-Yield) and [Vahid Sohrabloo](https://github.com/vahid-sohrabloo).

See [AUTHORS](AUTHORS) for the complete list of contributors.
