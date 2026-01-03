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

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/jackc/puddle/v2"
	"github.com/valyala/bytebufferpool"
)

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrInvalidPollingDuration is returned when discovery polling is invalid
	ErrInvalidPollingDuration = errors.New("memcache: discovery polling duration is invalid")

	// ErrClusterConfigMiss means that GetConfig failed as cluster config was not present
	ErrClusterConfigMiss = errors.New("memcache: cluster config miss")
	// ErrCorruptGetResult corrupt get result read
	ErrCorruptGetResult = errors.New("memcache: corrupt get result read")
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 500 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

const buffered = 8 // arbitrary buffered channel size, for readability

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

func legalKey(key []byte) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

var (
	crlf            = []byte("\r\n")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	versionPrefix           = []byte("VERSION")
)

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) (*Client, error) {
	ss := new(ServerList)
	err := ss.SetServers(server...)
	if err != nil {
		return nil, err
	}
	return NewFromSelector(ss), nil
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// stop is a function type for stopping the discovery polling
type stop func()

// NewDiscoveryClient returns a discovery config enabled client which polls
// periodically for new information and updates server list if new information is found.
// All the servers which are found are used with equal weight.
// discoveryAddress should be in following form "ipv4-address:port"
// Note: pollingDuration should be at least 1 second.
func NewDiscoveryClient(discoveryAddress string, pollingDuration time.Duration) (*Client, error) {
	// Validate pollingDuration
	if pollingDuration.Seconds() < 1.0 {
		return nil, ErrInvalidPollingDuration
	}
	return newDiscoveryClient(discoveryAddress, pollingDuration)
}

// newDiscoveryClient is the internal implementation for unit tests
func newDiscoveryClient(discoveryAddress string, pollingDuration time.Duration) (*Client, error) {
	// creates a new ServerList object which contains all the server eventually.
	ss := new(ServerList)
	mcCfgPollerHelper, err := New(discoveryAddress)
	if err != nil {
		return nil, err
	}
	cfgPoller := newConfigPoller(pollingDuration, ss, mcCfgPollerHelper)
	// cfgPoller starts polling immediately.
	mcClient := NewFromSelector(ss)
	mcClient.StopPolling = cfgPoller.stopPolling
	return mcClient, nil
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	MaxIdleConns int

	selector ServerSelector

	// StopPolling stops the discovery polling. Only set for discovery-enabled clients.
	StopPolling stop

	mu    sync.Mutex
	pools map[string]*puddle.Pool[*conn]
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key []byte

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// CasID is the compare and swap ID.
	//
	// It's populated by get requests and then the same value is
	// required for a CompareAndSwap request to succeed.
	CasID uint64
}

// reset resets the Item fields for reuse
func (it *Item) Reset() {
	it.Key = it.Key[:0]
	it.Value = it.Value[:0]
	it.Flags = 0
	it.Expiration = 0
	it.CasID = 0
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// setDeadlines sets both read and write deadlines on the connection.
// Use this when you need both operations to have the same timeout.
func (cn *conn) setDeadlines() {
	timeout := cn.c.netTimeout()
	//nolint:errcheck
	cn.nc.SetDeadline(time.Now().Add(timeout))
}

// condRelease releases this connection back to the puddle pool unless the
// error is non-resumable, in which case the resource is destroyed.
func (cn *conn) condRelease(res *puddle.Resource[*conn], err error) {
	if err == nil || resumableError(err) {
		// Clear both read and write deadlines before returning to pool.
		// This prevents idle connections from expiring while sitting in the pool
		// and avoids stale deadline issues on connection reuse.
		//nolint:errcheck
		cn.nc.SetReadDeadline(time.Time{})
		//nolint:errcheck
		cn.nc.SetWriteDeadline(time.Time{})
		res.Release()
		return
	}
	res.Destroy()
}

func (c *Client) getPool(addr net.Addr) (*puddle.Pool[*conn], error) {
	key := addr.String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pools == nil {
		c.pools = make(map[string]*puddle.Pool[*conn])
	}
	if pool, ok := c.pools[key]; ok {
		return pool, nil
	}
	pool, err := puddle.NewPool(&puddle.Config[*conn]{
		Constructor: func(ctx context.Context) (*conn, error) {
			nc, err := c.dial(addr)
			if err != nil {
				return nil, err
			}
			return &conn{
				nc:   nc,
				rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
				addr: addr,
				c:    c,
			}, nil
		},
		Destructor: func(cn *conn) {
			_ = cn.nc.Close()
		},
		MaxSize: int32(c.maxIdleConns()),
	})
	if err != nil {
		return nil, err
	}
	c.pools[key] = pool
	return pool, nil
}

func (c *Client) acquireConn(addr net.Addr) (*puddle.Resource[*conn], error) {
	pool, err := c.getPool(addr)
	if err != nil {
		return nil, err
	}
	// todo get from input
	res, err := pool.Acquire(context.Background())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &ConnectTimeoutError{addr}
		}
		return nil, err
	}
	// Set deadlines for both read and write operations upfront.
	// This single call covers the entire operation lifecycle.
	res.Value().setDeadlines()
	return res, nil
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) maxIdleConns() int {
	if c.MaxIdleConns > 0 {
		return c.MaxIdleConns
	}
	return DefaultMaxIdleConns
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	conn, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, &ConnectTimeoutError{addr}
		}
		return nil, err
	}
	return conn, nil
}

func (c *Client) onItem(item *Item, fn func(*Client, *conn, *Item) error) error {
	addr, err := c.selector.PickServer(item.Key)
	if err != nil {
		return err
	}
	res, err := c.acquireConn(addr)
	if err != nil {
		return err
	}
	cn := res.Value()
	defer cn.condRelease(res, err)
	return fn(c, cn, item)
}

func (c *Client) FlushAll() error {
	return c.selector.Each(c.flushAllFromAddr)
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key []byte) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, [][]byte{key}, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// GetWithItem gets the item for the given key and populates the provided item.
// This allows the caller to reuse an Item instance to avoid allocations.
// ErrCacheMiss is returned for a memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) GetWithItem(key []byte, item *Item) error {
	found := false
	err := c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddrWithItem(addr, [][]byte{key}, item, func(it *Item) { found = true })
	})
	if err == nil && !found {
		err = ErrCacheMiss
	}
	return err
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. Zero means the item has
// no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *Client) Touch(key []byte, seconds int32) (err error) {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.touchFromAddr(addr, key, seconds)
	})
}

func (c *Client) withKeyAddr(key []byte, fn func(net.Addr) error) (err error) {
	if !legalKey(key) {
		return ErrMalformedKey
	}
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return err
	}
	return fn(addr)
}

func (c *Client) withAddrRw(addr net.Addr, fn func(*conn) error) (err error) {
	res, err := c.acquireConn(addr)
	if err != nil {
		return err
	}
	cn := res.Value()
	defer cn.condRelease(res, err)
	return fn(cn)
}

func (c *Client) withKeyRw(key []byte, fn func(*conn) error) error {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRw(addr, fn)
	})
}

func (c *Client) getFromAddr(addr net.Addr, keys [][]byte, cb func(*Item)) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		buf := bytebufferpool.Get()
		//nolint:errcheck
		buf.WriteString("gets")
		for _, key := range keys {
			//nolint:errcheck
			buf.WriteByte(' ')
			//nolint:errcheck
			buf.Write(key)
		}
		//nolint:errcheck
		buf.WriteString("\r\n")

		if _, err := cn.rw.Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw.Reader, cn, nil, cb)
	})
}

func (c *Client) getFromAddrWithItem(addr net.Addr, keys [][]byte, item *Item, cb func(*Item)) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		//nolint:errcheck
		cn.rw.WriteString("gets")
		for _, key := range keys {
			//nolint:errcheck
			cn.rw.WriteByte(' ')
			//nolint:errcheck
			cn.rw.Write(key)
		}
		//nolint:errcheck
		cn.rw.WriteString("\r\n")

		if err := cn.rw.Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw.Reader, cn, item, cb)
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		if _, err := cn.rw.WriteString("flush_all\r\n"); err != nil {
			return err
		}
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		line, err := cn.rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(line, resultOk) {
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

// ping sends the version command to the given addr
func (c *Client) ping(addr net.Addr) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		if _, err := cn.rw.WriteString("version\r\n"); err != nil {
			return err
		}
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		line, err := cn.rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(line, versionPrefix) {
			return fmt.Errorf("memcache: unexpected response line from version: %q", string(line))
		}
		return nil
	})
}

func (c *Client) touchFromAddr(addr net.Addr, key []byte, expiration int32) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		buf := bytebufferpool.Get()
		//nolint:errcheck
		buf.WriteString("touch ")
		//nolint:errcheck
		buf.Write(key)
		//nolint:errcheck
		buf.WriteByte(' ')
		buf.B = strconv.AppendInt(buf.B, int64(expiration), 10)
		//nolint:errcheck
		buf.WriteString("\r\n")

		if _, err := cn.rw.Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		line, err := cn.rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		var result error
		switch {
		case bytes.Equal(line, resultTouched):
			result = nil
		case bytes.Equal(line, resultNotFound):
			result = ErrCacheMiss
		default:
			result = fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
		}
		return result
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys [][]byte) (map[string]*Item, error) {
	var mu sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		mu.Lock()
		defer mu.Unlock()
		m[string(it.Key)] = it
	}

	keyMap := make(map[net.Addr][][]byte)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, ErrMalformedKey
		}
		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys [][]byte) {
			ch <- c.getFromAddr(addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	errf := func(line []byte) (int, error) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	if !bytes.HasPrefix(line, []byte("VALUE ")) || !bytes.HasSuffix(line, []byte("\r\n")) {
		return errf(line)
	}
	s := line[6 : len(line)-2]
	var rest []byte
	var found bool
	keySlice, rest, found := cut(s, ' ')
	if !found {
		return errf(line)
	}
	// Copy the key since line may be from ReadSlice which reuses the buffer
	it.Key = append(it.Key[:0], keySlice...)
	
	val, rest, found := cut(rest, ' ')
	if !found {
		return errf(line)
	}
	flags64, err := strconv.ParseUint(b2s(val), 10, 32)
	if err != nil {
		return errf(line)
	}
	it.Flags = uint32(flags64)
	val, rest, found = cut(rest, ' ')
	size64, err := strconv.ParseUint(b2s(val), 10, 32)
	if err != nil {
		return errf(line)
	}
	if size64 > math.MaxInt { // Can happen if int is 32-bit
		return errf(line)
	}
	if !found { // final CAS ID is optional.
		return int(size64), nil
	}
	it.CasID, err = strconv.ParseUint(b2s(rest), 10, 64)
	if err != nil {
		return errf(line)
	}
	return int(size64), nil
}

// Similar to strings.Cut in Go 1.18, but sep can only be 1 byte.
func cut(s []byte, sep byte) (before, after []byte, found bool) {
	if i := bytes.IndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, nil, false
}

// parseGetResponse reads a GET response using bufio.Reader
// and calls cb for each read and allocated Item.
func parseGetResponse(r *bufio.Reader, cn *conn, providedItem *Item, cb func(*Item)) error {
	for {
		// No need to extend deadline in the loop - the deadline set at
		// connection acquisition covers the entire operation. This eliminates
		// excessive syscalls, especially for multi-get operations with many items.

		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		var it *Item
		if providedItem != nil {
			it = providedItem
		} else {
			it = new(Item)
		}
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return err
		}

		neededSize := size + 2
		it.Value = it.Value[:0]
		it.Value = slices.Grow(it.Value, neededSize)
		it.Value = it.Value[:neededSize]
		// Read the value data
		if _, err := io.ReadFull(r, it.Value); err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return ErrCorruptGetResult
		}
		// Copy the value to the item
		if cap(it.Value) < size {
			it.Value = make([]byte, size)
		} else {
			it.Value = it.Value[:size]
		}
		cb(it)
	}
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.onItem(item, (*Client).set)
}

func (c *Client) set(cn *conn, item *Item) error {
	return c.populateOne(cn, "set", item)
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.onItem(item, (*Client).add)
}

func (c *Client) add(cn *conn, item *Item) error {
	return c.populateOne(cn, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Client) Replace(item *Item) error {
	return c.onItem(item, (*Client).replace)
}

func (c *Client) replace(cn *conn, item *Item) error {
	return c.populateOne(cn, "replace", item)
}

// Append appends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Append(item *Item) error {
	return c.onItem(item, (*Client).append)
}

func (c *Client) append(cn *conn, item *Item) error {
	return c.populateOne(cn, "append", item)
}

// Prepend prepends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Prepend(item *Item) error {
	return c.onItem(item, (*Client).prepend)
}

func (c *Client) prepend(cn *conn, item *Item) error {
	return c.populateOne(cn, "prepend", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.onItem(item, (*Client).cas)
}

func (c *Client) cas(cn *conn, item *Item) error {
	return c.populateOne(cn, "cas", item)
}

func (*Client) populateOne(cn *conn, verb string, item *Item) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}

	// Get buffer from pool
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// Build command in buffer
	buf.B = append(buf.B, verb...)
	buf.B = append(buf.B, ' ')
	buf.B = append(buf.B, item.Key...)
	buf.B = append(buf.B, ' ')
	buf.B = strconv.AppendUint(buf.B, uint64(item.Flags), 10)
	buf.B = append(buf.B, ' ')
	buf.B = strconv.AppendInt(buf.B, int64(item.Expiration), 10)
	buf.B = append(buf.B, ' ')
	buf.B = strconv.AppendInt(buf.B, int64(len(item.Value)), 10)
	if verb == "cas" {
		buf.B = append(buf.B, ' ')
		buf.B = strconv.AppendUint(buf.B, item.CasID, 10)
	}
	buf.B = append(buf.B, crlf...)
	buf.B = append(buf.B, item.Value...)
	buf.B = append(buf.B, crlf...)

	if _, err := cn.rw.Write(buf.B); err != nil {
		return err
	}
	if err := cn.rw.Flush(); err != nil {
		return err
	}
	line, err := cn.rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	var result error
	switch {
	case bytes.Equal(line, resultStored):
		result = nil
	case bytes.Equal(line, resultNotStored):
		result = ErrNotStored
	case bytes.Equal(line, resultExists):
		result = ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		result = ErrCacheMiss
	default:
		result = fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
	}
	return result
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(key []byte) error {
	return c.withKeyRw(key, func(cn *conn) error {
		buf := bytebufferpool.Get()
		//nolint:errcheck
		buf.WriteString("delete ")
		//nolint:errcheck
		buf.Write(key)
		//nolint:errcheck
		buf.WriteString("\r\n")

		if _, err := cn.rw.Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		line, err := cn.rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		var result error
		switch {
		case bytes.Equal(line, resultOK):
			result = nil
		case bytes.Equal(line, resultDeleted):
			result = nil
		case bytes.Equal(line, resultNotStored):
			result = ErrNotStored
		case bytes.Equal(line, resultExists):
			result = ErrCASConflict
		case bytes.Equal(line, resultNotFound):
			result = ErrCacheMiss
		default:
			result = fmt.Errorf("memcache: unexpected response line: %q", string(line))
		}
		return result
	})
}

// DeleteAll deletes all items in the cache.
func (c *Client) DeleteAll() error {
	return c.withKeyRw([]byte(""), func(cn *conn) error {
		if _, err := cn.rw.WriteString("flush_all\r\n"); err != nil {
			return err
		}
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		line, err := cn.rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		var result error
		switch {
		case bytes.Equal(line, resultOK):
			result = nil
		case bytes.Equal(line, resultDeleted):
			result = nil
		case bytes.Equal(line, resultNotStored):
			result = ErrNotStored
		case bytes.Equal(line, resultExists):
			result = ErrCASConflict
		case bytes.Equal(line, resultNotFound):
			result = ErrCacheMiss
		default:
			result = fmt.Errorf("memcache: unexpected response line: %q", string(line))
		}
		return result
	})
}

// Get and Touch the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) GetAndTouch(key []byte, expiration int32) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getAndTouchFromAddr(addr, key, expiration, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// GetAndTouchWithItem gets and touches the item with the provided key and populates the provided item.
// This allows the caller to reuse an Item instance to avoid allocations.
// The error ErrCacheMiss is returned if the item didn't already exist in the cache.
func (c *Client) GetAndTouchWithItem(key []byte, expiration int32, item *Item) error {
	found := false
	err := c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getAndTouchFromAddrWithItem(addr, key, expiration, item, func(it *Item) { found = true })
	})
	if err == nil && !found {
		err = ErrCacheMiss
	}
	return err
}

func (c *Client) getAndTouchFromAddr(addr net.Addr, key []byte, expiration int32, cb func(*Item)) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		buf := bytebufferpool.Get()
		//nolint:errcheck
		buf.WriteString("gat ")
		buf.B = strconv.AppendInt(buf.B, int64(expiration), 10)
		//nolint:errcheck
		buf.WriteByte(' ')
		//nolint:errcheck
		buf.Write(key)
		//nolint:errcheck
		buf.WriteString("\r\n")

		if _, err := cn.rw.Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw.Reader, cn, nil, cb)
	})
}

func (c *Client) getAndTouchFromAddrWithItem(addr net.Addr, key []byte, expiration int32, item *Item, cb func(*Item)) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		buf := bytebufferpool.Get()
		//nolint:errcheck
		buf.WriteString("gat ")
		buf.B = strconv.AppendInt(buf.B, int64(expiration), 10)
		//nolint:errcheck
		buf.WriteByte(' ')
		//nolint:errcheck
		buf.Write(key)
		//nolint:errcheck
		buf.WriteString("\r\n")

		if _, err := cn.rw.Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw.Reader, cn, item, cb)
	})
}

// Ping checks all instances if they are alive. Returns error if any
// of them is down.
func (c *Client) Ping() error {
	return c.selector.Each(c.ping)
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key []byte, delta uint64) (newValue uint64, err error) {
	return c.incrDecr([]byte("incr"), key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key []byte, delta uint64) (newValue uint64, err error) {
	return c.incrDecr([]byte("decr"), key, delta)
}

func (c *Client) incrDecr(verb, key []byte, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(cn *conn) error {
		buf := bytebufferpool.Get()
		//nolint:errcheck
		buf.Write(verb)
		//nolint:errcheck
		buf.WriteByte(' ')
		//nolint:errcheck
		buf.Write(key)
		//nolint:errcheck
		buf.WriteByte(' ')
		buf.B = strconv.AppendUint(buf.B, delta, 10)
		//nolint:errcheck
		buf.WriteString("\r\n")

		if _, err := cn.rw.Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		line, err := cn.rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		return err
	})
	return val, err
}

// Close closes any open connections.
//
// It returns the first error encountered closing connections, but always
// closes all connections.
//
// After Close, the Client may still be used.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pool := range c.pools {
		pool.Close()
	}
	c.pools = nil
	return nil
}

// GetConfig gets the config type. ErrClusterConfigMiss is returned if config
// for the type cluster is not found. The configType must be at most 250 bytes in length.
func (c *Client) GetConfig(configType string) (clusterConfig *ClusterConfig, err error) {
	clusterConfig, err = c.getConfig(configType)
	if err != nil {
		return nil, err
	}

	if clusterConfig == nil {
		return nil, ErrClusterConfigMiss
	}

	return clusterConfig, nil
}

// getConfig gets the config type. ErrClusterConfigMiss is returned if config
// for the type cluster is not found. The configType must be at most 250 bytes in length.
func (c *Client) getConfig(configType string) (clusterConfig *ClusterConfig, err error) {
	addr, err := c.selector.PickAnyServer()
	if err != nil {
		return nil, err
	}
	err = c.getConfigFromAddr(addr, configType, func(cc *ClusterConfig) { clusterConfig = cc })
	if err != nil {
		return nil, err
	}
	if clusterConfig == nil {
		err = ErrClusterConfigMiss
	}
	return
}

func (c *Client) getConfigFromAddr(addr net.Addr, configType string, cb func(*ClusterConfig)) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		cmd := fmt.Sprintf("config get %s\r\n", configType)

		if _, err := cn.rw.WriteString(cmd); err != nil {
			return err
		}
		if err := cn.rw.Flush(); err != nil {
			return err
		}
		return parseConfigGetResponse(cn.rw.Reader, cb)
	})
}

func b2s(input []byte) string {
	return unsafe.String(&input[0], len(input))
}
