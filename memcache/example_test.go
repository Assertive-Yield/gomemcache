package memcache_test

import (
	"fmt"

	"github.com/Assertive-Yield/gomemcache/memcache"
)

// Example demonstrating the new GetWithItem API that allows
// reusing Item instances to avoid allocations.
func ExampleClient_GetWithItem() {
	mc, err := memcache.New("localhost:11211")
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	// Set an item
	err = mc.Set(&memcache.Item{Key: []byte("foo"), Value: []byte("my value")})
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	// Allocate an Item once and reuse it
	item := &memcache.Item{}

	// First get - populates the item
	err = mc.GetWithItem([]byte("foo"), item)
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	fmt.Printf("First get: %s\n", item.Value)

	// Reuse the same item for another get
	err = mc.GetWithItem([]byte("foo"), item)
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	fmt.Printf("Second get: %s\n", item.Value)
}

// Example demonstrating GetAndTouchWithItem.
func ExampleClient_GetAndTouchWithItem() {
	mc, err := memcache.New("localhost:11211")
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	// Set an item with 60 second expiration
	err = mc.Set(&memcache.Item{Key: []byte("bar"), Value: []byte("test value"), Expiration: 60})
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	// Allocate an Item once and reuse it
	item := &memcache.Item{}

	// Get and extend expiration to 120 seconds
	err = mc.GetAndTouchWithItem([]byte("bar"), 120, item)
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	fmt.Printf("Value: %s\n", item.Value)
}
