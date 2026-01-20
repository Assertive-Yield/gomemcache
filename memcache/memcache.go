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
	"math/rand"
	"net"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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

// Pool configuration defaults
var (
	defaultMaxConns          = int32(200)
	defaultMinConns          = int32(100)
	defaultMaxConnLifetime   = time.Hour
	defaultMaxConnIdleTime   = time.Minute * 30
	defaultHealthCheckPeriod = time.Minute
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
	return &Client{
		selector:          ss,
		maxConns:          defaultMaxConns,
		minConns:          defaultMinConns,
		maxConnLifetime:   defaultMaxConnLifetime,
		maxConnIdleTime:   defaultMaxConnIdleTime,
		healthCheckPeriod: defaultHealthCheckPeriod,
		healthCheckChan:   make(chan struct{}, 1),
		closeChan:         make(chan struct{}),
	}
}

// NewWithConfig creates a new Client with the provided configuration.
// config must have been created by NewConfig.
func NewWithConfig(config *Config) (*Client, error) {
	if !config.createdByNewConfig {
		panic("config must be created by NewConfig")
	}

	ss := new(ServerList)
	err := ss.SetServers(config.Servers...)
	if err != nil {
		return nil, err
	}

	c := &Client{
		selector:              ss,
		Timeout:               config.Timeout,
		config:                config,
		beforeConnect:         config.BeforeConnect,
		afterConnect:          config.AfterConnect,
		beforeAcquire:         config.BeforeAcquire,
		afterRelease:          config.AfterRelease,
		beforeClose:           config.BeforeClose,
		minConns:              config.MinConns,
		maxConns:              config.MaxConns,
		maxConnLifetime:       config.MaxConnLifetime,
		maxConnLifetimeJitter: config.MaxConnLifetimeJitter,
		maxConnIdleTime:       config.MaxConnIdleTime,
		healthCheckPeriod:     config.HealthCheckPeriod,
		healthCheckChan:       make(chan struct{}, 1),
		closeChan:             make(chan struct{}),
	}

	// Start background health check if health check period is set
	if c.healthCheckPeriod > 0 {
		go c.backgroundHealthCheck()
	}

	return c, nil
}

// Config returns a copy of config that was used to initialize this client.
// Returns nil if the client was not created with NewWithConfig.
func (c *Client) Config() *Config {
	if c.config == nil {
		return nil
	}
	return c.config.Copy()
}

// Stat returns a Stat struct with a snapshot of pool statistics for all servers.
// Note: This aggregates stats across all server pools.
func (c *Client) Stat() *Stat {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Aggregate stats from all pools
	var totalStat *puddle.Stat
	for _, pool := range c.pools {
		if totalStat == nil {
			s := pool.Stat()
			totalStat = s
		}
		// Note: puddle.Stat doesn't support aggregation, so we return first pool's stat
		// For more detailed stats, use StatForServer
	}

	if totalStat == nil {
		totalStat = &puddle.Stat{}
	}

	return &Stat{
		s:                    totalStat,
		newConnsCount:        atomic.LoadInt64(&c.newConnsCount),
		lifetimeDestroyCount: atomic.LoadInt64(&c.lifetimeDestroyCount),
		idleDestroyCount:     atomic.LoadInt64(&c.idleDestroyCount),
	}
}

// Reset closes all connections, but leaves the client open. It is intended for use when an error is detected that would
// disrupt all connections (such as a network interruption or a server state change).
//
// It is safe to reset the client while connections are checked out. Those connections will be closed when they are returned
// to the pool.
func (c *Client) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pool := range c.pools {
		pool.Reset()
	}
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

// Config is the configuration struct for creating a Client with pool settings.
// It must be created by NewConfig and then it can be modified.
type Config struct {
	// Servers is the list of memcache server addresses.
	Servers []string

	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// BeforeConnect is called before a new connection is made. It is passed a copy of the address.
	// If this returns an error, the connection attempt fails.
	BeforeConnect func(context.Context, net.Addr) error

	// AfterConnect is called after a connection is established, but before it is added to the pool.
	// It can be used to perform initial setup on the connection.
	AfterConnect func(context.Context, net.Conn) error

	// BeforeAcquire is called before a connection is acquired from the pool. It must return true to allow the
	// acquisition or false to indicate that the connection should be destroyed and a different connection should be
	// acquired.
	BeforeAcquire func(context.Context, net.Conn) bool

	// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
	// return the connection to the pool or false to destroy the connection.
	AfterRelease func(net.Conn) bool

	// BeforeClose is called right before a connection is closed and removed from the pool.
	BeforeClose func(net.Conn)

	// MaxConnLifetime is the duration since creation after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter is the duration after MaxConnLifetime to randomly decide to close a connection.
	// This helps prevent all connections from being closed at the exact same time, starving the pool.
	MaxConnLifetimeJitter time.Duration

	// MaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
	MaxConnIdleTime time.Duration

	// MaxConns is the maximum size of the pool per server. The default is the greater of 4 or runtime.NumCPU().
	MaxConns int32

	// MinConns is the minimum size of the pool per server. After connection closes, the pool might dip below MinConns. A low
	// number of MinConns might mean the pool is empty after MaxConnLifetime until the health check has a chance
	// to create new connections.
	MinConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration

	createdByNewConfig bool // Used to enforce created by NewConfig rule.
}

// NewConfig creates a new Config with default values.
// The returned Config can be modified before passing to NewWithConfig.
func NewConfig(servers ...string) *Config {
	maxConns := defaultMaxConns
	if numCPU := int32(runtime.NumCPU()); numCPU > maxConns {
		maxConns = numCPU
	}

	return &Config{
		Servers:            servers,
		Timeout:            DefaultTimeout,
		MaxConns:           maxConns,
		MinConns:           defaultMinConns,
		MaxConnLifetime:    defaultMaxConnLifetime,
		MaxConnIdleTime:    defaultMaxConnIdleTime,
		HealthCheckPeriod:  defaultHealthCheckPeriod,
		createdByNewConfig: true,
	}
}

// Copy returns a deep copy of the config that is safe to use and modify.
func (c *Config) Copy() *Config {
	newConfig := new(Config)
	*newConfig = *c
	if c.Servers != nil {
		newConfig.Servers = make([]string, len(c.Servers))
		copy(newConfig.Servers, c.Servers)
	}
	return newConfig
}

// connResource wraps a connection for pool management
type connResource struct {
	nc         net.Conn
	rw         *bufio.ReadWriter
	addr       net.Addr
	maxAgeTime time.Time
}

// Stat is a snapshot of pool statistics.
type Stat struct {
	s                    *puddle.Stat
	newConnsCount        int64
	lifetimeDestroyCount int64
	idleDestroyCount     int64
}

// AcquireCount returns the cumulative count of successful acquires from the pool.
func (s *Stat) AcquireCount() int64 {
	return s.s.AcquireCount()
}

// AcquireDuration returns the total duration of all successful acquires from the pool.
func (s *Stat) AcquireDuration() time.Duration {
	return s.s.AcquireDuration()
}

// AcquiredConns returns the number of currently acquired connections in the pool.
func (s *Stat) AcquiredConns() int32 {
	return s.s.AcquiredResources()
}

// CanceledAcquireCount returns the cumulative count of acquires from the pool that were canceled.
func (s *Stat) CanceledAcquireCount() int64 {
	return s.s.CanceledAcquireCount()
}

// ConstructingConns returns the number of conns with construction in progress in the pool.
func (s *Stat) ConstructingConns() int32 {
	return s.s.ConstructingResources()
}

// EmptyAcquireCount returns the cumulative count of successful acquires from the pool
// that waited for a resource to be released or constructed because the pool was empty.
func (s *Stat) EmptyAcquireCount() int64 {
	return s.s.EmptyAcquireCount()
}

// IdleConns returns the number of currently idle conns in the pool.
func (s *Stat) IdleConns() int32 {
	return s.s.IdleResources()
}

// MaxConns returns the maximum size of the pool.
func (s *Stat) MaxConns() int32 {
	return s.s.MaxResources()
}

// TotalConns returns the total number of resources currently in the pool.
func (s *Stat) TotalConns() int32 {
	return s.s.TotalResources()
}

// NewConnsCount returns the cumulative count of new connections opened.
func (s *Stat) NewConnsCount() int64 {
	return s.newConnsCount
}

// MaxLifetimeDestroyCount returns the cumulative count of connections destroyed because they exceeded MaxConnLifetime.
func (s *Stat) MaxLifetimeDestroyCount() int64 {
	return s.lifetimeDestroyCount
}

// MaxIdleDestroyCount returns the cumulative count of connections destroyed because they exceeded MaxConnIdleTime.
func (s *Stat) MaxIdleDestroyCount() int64 {
	return s.idleDestroyCount
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

	// Pool configuration
	config *Config

	// Pool callbacks
	beforeConnect func(context.Context, net.Addr) error
	afterConnect  func(context.Context, net.Conn) error
	beforeAcquire func(context.Context, net.Conn) bool
	afterRelease  func(net.Conn) bool
	beforeClose   func(net.Conn)

	// Pool settings
	minConns              int32
	maxConns              int32
	maxConnLifetime       time.Duration
	maxConnLifetimeJitter time.Duration
	maxConnIdleTime       time.Duration
	healthCheckPeriod     time.Duration

	// Pool statistics
	newConnsCount        int64
	lifetimeDestroyCount int64
	idleDestroyCount     int64

	// Health check channels
	healthCheckChan chan struct{}
	closeOnce       sync.Once
	closeChan       chan struct{}

	mu    sync.Mutex
	pools map[string]*puddle.Pool[*connResource]
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

// conn is a connection wrapper for operations.
type conn struct {
	cr  *connResource
	res *puddle.Resource[*connResource]
	c   *Client
}

// rw returns the buffered reader/writer for this connection.
func (cn *conn) rw() *bufio.ReadWriter {
	return cn.cr.rw
}

// setDeadlines sets both read and write deadlines on the connection.
// Use this when you need both operations to have the same timeout.
func (cn *conn) setDeadlines() {
	timeout := cn.c.netTimeout()
	//nolint:errcheck
	cn.cr.nc.SetDeadline(time.Now().Add(timeout))
}

// condRelease releases this connection back to the puddle pool unless the
// error is non-resumable, in which case the resource is destroyed.
func (cn *conn) condRelease(err error) {
	if cn.res == nil {
		return
	}

	res := cn.res
	cn.res = nil

	// Check if connection should be destroyed
	if cn.cr.nc == nil || !resumableError(err) && err != nil {
		if cn.c.beforeClose != nil {
			cn.c.beforeClose(cn.cr.nc)
		}
		res.Destroy()
		cn.c.triggerHealthCheck()
		return
	}

	// Check if connection has exceeded its lifetime
	if cn.c.isExpired(res) {
		atomic.AddInt64(&cn.c.lifetimeDestroyCount, 1)
		if cn.c.beforeClose != nil {
			cn.c.beforeClose(cn.cr.nc)
		}
		res.Destroy()
		cn.c.triggerHealthCheck()
		return
	}

	// Check afterRelease callback
	if cn.c.afterRelease != nil && !cn.c.afterRelease(cn.cr.nc) {
		if cn.c.beforeClose != nil {
			cn.c.beforeClose(cn.cr.nc)
		}
		res.Destroy()
		cn.c.triggerHealthCheck()
		return
	}

	// Clear both read and write deadlines before returning to pool.
	// This prevents idle connections from expiring while sitting in the pool
	// and avoids stale deadline issues on connection reuse.
	//nolint:errcheck
	cn.cr.nc.SetReadDeadline(time.Time{})
	//nolint:errcheck
	cn.cr.nc.SetWriteDeadline(time.Time{})
	res.Release()
}

func (c *Client) getPool(addr net.Addr) (*puddle.Pool[*connResource], error) {
	key := addr.String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pools == nil {
		c.pools = make(map[string]*puddle.Pool[*connResource])
	}
	if pool, ok := c.pools[key]; ok {
		return pool, nil
	}

	maxSize := c.maxConns
	if maxSize <= 0 {
		maxSize = int32(c.maxIdleConns())
	}

	pool, err := puddle.NewPool(&puddle.Config[*connResource]{
		Constructor: func(ctx context.Context) (*connResource, error) {
			atomic.AddInt64(&c.newConnsCount, 1)

			// Call beforeConnect callback
			if c.beforeConnect != nil {
				if err := c.beforeConnect(ctx, addr); err != nil {
					return nil, err
				}
			}

			nc, err := c.dial(addr)
			if err != nil {
				return nil, err
			}

			// Call afterConnect callback
			if c.afterConnect != nil {
				if err := c.afterConnect(ctx, nc); err != nil {
					//nolint:errcheck
					_ = nc.Close()
					return nil, err
				}
			}

			// Calculate max age time with jitter
			//nolint:gosec // rand is not used for security purposes
			jitterSecs := rand.Float64() * c.maxConnLifetimeJitter.Seconds()
			maxAgeTime := time.Now().Add(c.maxConnLifetime).Add(time.Duration(jitterSecs) * time.Second)

			return &connResource{
				nc:         nc,
				rw:         bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
				addr:       addr,
				maxAgeTime: maxAgeTime,
			}, nil
		},
		Destructor: func(cr *connResource) {
			if c.beforeClose != nil {
				c.beforeClose(cr.nc)
			}
			_ = cr.nc.Close()
		},
		MaxSize: maxSize,
	})
	if err != nil {
		return nil, err
	}
	c.pools[key] = pool
	return pool, nil
}

// isExpired checks if a connection resource has exceeded its maximum lifetime
func (c *Client) isExpired(res *puddle.Resource[*connResource]) bool {
	return time.Now().After(res.Value().maxAgeTime)
}

// triggerHealthCheck signals the health check goroutine to run
func (c *Client) triggerHealthCheck() {
	go func() {
		// Destroy is asynchronous so we give it time to actually remove itself from
		// the pool otherwise we might try to check the pool size too soon
		time.Sleep(500 * time.Millisecond)
		select {
		case c.healthCheckChan <- struct{}{}:
		default:
		}
	}()
}

// backgroundHealthCheck runs periodic health checks on idle connections
func (c *Client) backgroundHealthCheck() {
	ticker := time.NewTicker(c.healthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			return
		case <-c.healthCheckChan:
			c.checkHealth()
		case <-ticker.C:
			c.checkHealth()
		}
	}
}

// checkHealth performs health check on all pools
func (c *Client) checkHealth() {
	for {
		// If checkMinConns failed we don't destroy any connections since we couldn't
		// even get to minConns
		if err := c.checkMinConns(); err != nil {
			break
		}
		if !c.checkConnsHealth() {
			// Since we didn't destroy any connections we can stop looping
			break
		}
		// Technically Destroy is asynchronous but 500ms should be enough for it to
		// remove it from the underlying pool
		select {
		case <-c.closeChan:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// checkConnsHealth checks all idle connections and destroys those that are expired or idle too long
func (c *Client) checkConnsHealth() bool {
	c.mu.Lock()
	pools := make([]*puddle.Pool[*connResource], 0, len(c.pools))
	for _, pool := range c.pools {
		pools = append(pools, pool)
	}
	c.mu.Unlock()

	var destroyed bool
	for _, pool := range pools {
		totalConns := pool.Stat().TotalResources()
		resources := pool.AcquireAllIdle()
		for _, res := range resources {
			// We're okay going under minConns if the lifetime is up
			if c.isExpired(res) && totalConns >= c.minConns {
				atomic.AddInt64(&c.lifetimeDestroyCount, 1)
				res.Destroy()
				destroyed = true
				totalConns--
			} else if res.IdleDuration() > c.maxConnIdleTime && totalConns > c.minConns {
				atomic.AddInt64(&c.idleDestroyCount, 1)
				res.Destroy()
				destroyed = true
				totalConns--
			} else {
				res.ReleaseUnused()
			}
		}
	}
	return destroyed
}

// checkMinConns ensures minimum connections are maintained in all pools
func (c *Client) checkMinConns() error {
	c.mu.Lock()
	pools := make([]*puddle.Pool[*connResource], 0, len(c.pools))
	for _, pool := range c.pools {
		pools = append(pools, pool)
	}
	c.mu.Unlock()

	for _, pool := range pools {
		toCreate := c.minConns - pool.Stat().TotalResources()
		if toCreate > 0 {
			if err := c.createIdleResources(pool, int(toCreate)); err != nil {
				return err
			}
		}
	}
	return nil
}

// createIdleResources creates idle resources in the pool
func (c *Client) createIdleResources(pool *puddle.Pool[*connResource], targetResources int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, targetResources)

	for i := 0; i < targetResources; i++ {
		go func() {
			res, err := pool.Acquire(ctx)
			if err == nil {
				res.Release()
			}
			// Ignore ErrNotAvailable since that just means the pool is full
			if err == puddle.ErrNotAvailable {
				err = nil
			}
			errs <- err
		}()
	}

	var firstError error
	for i := 0; i < targetResources; i++ {
		err := <-errs
		if err != nil && firstError == nil {
			cancel()
			firstError = err
		}
	}
	return firstError
}

func (c *Client) acquireConn(addr net.Addr) (*conn, error) {
	pool, err := c.getPool(addr)
	if err != nil {
		return nil, err
	}

	for {
		// todo get from input
		res, err := pool.Acquire(context.Background())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, &ConnectTimeoutError{Addr: addr, Timeout: c.netTimeout()}
			}
			return nil, fmt.Errorf("acquire: %w", err)
		}

		cr := res.Value()

		// If connection has been idle for more than a second, ping to verify it's still alive
		if res.IdleDuration() > time.Second {
			// Quick check if connection is still valid
			if err := c.pingConn(cr); err != nil {
				res.Destroy()
				continue
			}
		}

		// Check beforeAcquire callback
		if c.beforeAcquire != nil && !c.beforeAcquire(context.Background(), cr.nc) {
			res.Destroy()
			continue
		}

		cn := &conn{
			cr:  cr,
			res: res,
			c:   c,
		}

		// Set deadlines for both read and write operations upfront.
		// This single call covers the entire operation lifecycle.
		cn.setDeadlines()
		return cn, nil
	}
}

// pingConn sends a version command to verify the connection is alive
func (c *Client) pingConn(cr *connResource) error {
	timeout := c.netTimeout()
	//nolint:errcheck
	cr.nc.SetDeadline(time.Now().Add(timeout))

	if _, err := cr.rw.WriteString("version\r\n"); err != nil {
		return err
	}
	if err := cr.rw.Flush(); err != nil {
		return err
	}
	line, err := cr.rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(line, versionPrefix) {
		return fmt.Errorf("memcache: unexpected response from version: %q", string(line))
	}
	return nil
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
	Addr    net.Addr
	Timeout time.Duration
}

func (cte *ConnectTimeoutError) Error() string {
	return fmt.Sprintf("memcache: connect timeout to %s after %v", cte.Addr.String(), cte.Timeout)
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	conn, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, &ConnectTimeoutError{Addr: addr, Timeout: c.netTimeout()}
		}
		return nil, err
	}
	return conn, nil
}

func (c *Client) onItem(item *Item, fn func(*Client, *conn, *Item) error) (err error) {
	addr, err := c.selector.PickServer(item.Key)
	if err != nil {
		return err
	}
	cn, err := c.acquireConn(addr)
	if err != nil {
		return err
	}
	defer func() { cn.condRelease(err) }()
	err = fn(c, cn, item)
	return err
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
	cn, err := c.acquireConn(addr)
	if err != nil {
		return err
	}
	defer func() { cn.condRelease(err) }()
	err = fn(cn)
	return err
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

		if _, err := cn.rw().Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw().Reader, cn, nil, cb)
	})
}

func (c *Client) getFromAddrWithItem(addr net.Addr, keys [][]byte, item *Item, cb func(*Item)) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		//nolint:errcheck
		cn.rw().WriteString("gets")
		for _, key := range keys {
			//nolint:errcheck
			cn.rw().WriteByte(' ')
			//nolint:errcheck
			cn.rw().Write(key)
		}
		//nolint:errcheck
		cn.rw().WriteString("\r\n")

		if err := cn.rw().Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw().Reader, cn, item, cb)
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(cn *conn) error {
		if _, err := cn.rw().WriteString("flush_all\r\n"); err != nil {
			return err
		}
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		line, err := cn.rw().ReadSlice('\n')
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
		if _, err := cn.rw().WriteString("version\r\n"); err != nil {
			return err
		}
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		line, err := cn.rw().ReadSlice('\n')
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

		if _, err := cn.rw().Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		line, err := cn.rw().ReadSlice('\n')
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
	if !bytes.HasPrefix(line, []byte("VALUE ")) || !bytes.HasSuffix(line, []byte("\r\n")) {
		return -1, fmt.Errorf("memcache: invalid get response format (expected 'VALUE ...'): %q", line)
	}
	s := line[6 : len(line)-2]
	var rest []byte
	var found bool
	keySlice, rest, found := cut(s, ' ')
	if !found {
		return -1, fmt.Errorf("memcache: missing key in get response: %q", line)
	}
	// Copy the key since line may be from ReadSlice which reuses the buffer
	it.Key = append(it.Key[:0], keySlice...)

	val, rest, found := cut(rest, ' ')
	if !found {
		return -1, fmt.Errorf("memcache: missing flags field in get response: %q", line)
	}
	flags64, err := strconv.ParseUint(b2s(val), 10, 32)
	if err != nil {
		return -1, fmt.Errorf("memcache: invalid flags value %q in get response: %q", val, line)
	}
	it.Flags = uint32(flags64)
	val, rest, found = cut(rest, ' ')
	if !found {
		return -1, fmt.Errorf("memcache: missing size field in get response: %q", line)
	}
	size64, err := strconv.ParseUint(b2s(val), 10, 32)
	if err != nil {
		return -1, fmt.Errorf("memcache: invalid size value %q in get response: %q", val, line)
	}
	if size64 > math.MaxInt { // Can happen if int is 32-bit
		return -1, fmt.Errorf("memcache: size value %d exceeds maximum allowed (%d) in get response: %q", size64, math.MaxInt, line)
	}
	if !found { // final CAS ID is optional.
		return int(size64), nil
	}
	it.CasID, err = strconv.ParseUint(b2s(rest), 10, 64)
	if err != nil {
		return -1, fmt.Errorf("memcache: invalid CAS ID %q in get response: %q", rest, line)
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

	if _, err := cn.rw().Write(buf.B); err != nil {
		return err
	}
	if err := cn.rw().Flush(); err != nil {
		return err
	}
	line, err := cn.rw().ReadSlice('\n')
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

		if _, err := cn.rw().Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		line, err := cn.rw().ReadSlice('\n')
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
		if _, err := cn.rw().WriteString("flush_all\r\n"); err != nil {
			return err
		}
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		line, err := cn.rw().ReadSlice('\n')
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

		if _, err := cn.rw().Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw().Reader, cn, nil, cb)
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

		if _, err := cn.rw().Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		return parseGetResponse(cn.rw().Reader, cn, item, cb)
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

		if _, err := cn.rw().Write(buf.B); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		bytebufferpool.Put(buf)
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		line, err := cn.rw().ReadSlice('\n')
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

// Close closes any open connections and stops background health checks.
//
// It returns the first error encountered closing connections, but always
// closes all connections.
//
// After Close, the Client may still be used.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})

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

		if _, err := cn.rw().WriteString(cmd); err != nil {
			return err
		}
		if err := cn.rw().Flush(); err != nil {
			return err
		}
		return parseConfigGetResponse(cn.rw().Reader, cb)
	})
}

func b2s(input []byte) string {
	return unsafe.String(&input[0], len(input))
}
