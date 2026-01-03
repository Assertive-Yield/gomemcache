/*
Copyright 2020 Google Inc.

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
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const emptyResponse = "END\r\n"

// fakeDiscoveryMemcacheServer is a fake discovery memcache server for testing.
type fakeDiscoveryMemcacheServer struct {
	discoveryResponseMutex sync.RWMutex

	// Usage instructions:
	// Either use (discoveryConfigID and discoveryPorts) OR discoveryConfigResponse
	// Using one clears other.
	discoveryConfigID       int
	discoveryPorts          []int
	discoveryConfigResponse string

	// Output only
	currentAddress string

	// internal bookeeping
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
}

// starts a tcp server on any free port.
func (s *fakeDiscoveryMemcacheServer) start(ctx context.Context) error {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	s.currentAddress = l.Addr().String()
	s.listener = l
	s.ctx, s.cancel = context.WithCancel(ctx)

	go s.listenForClients()

	return nil
}

func (s *fakeDiscoveryMemcacheServer) stop() error {
	if s.cancel != nil {
		s.cancel()
	}

	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

func (s *fakeDiscoveryMemcacheServer) listenForClients() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				continue
			}
		}

		go s.handleFakeDiscoveryMemcacheRequest(conn)
	}
}

func (s *fakeDiscoveryMemcacheServer) getResponseToSend() string {
	s.discoveryResponseMutex.RLock()
	defer s.discoveryResponseMutex.RUnlock()

	if s.discoveryConfigResponse != "" {
		return s.discoveryConfigResponse
	}

	if s.discoveryConfigID == 0 {
		return emptyResponse
	}

	var result strings.Builder

	result.WriteString("CONFIG cluster 0 80\r\n")
	result.WriteString(strconv.Itoa(s.discoveryConfigID))
	result.WriteString("\r\n")

	for i, port := range s.discoveryPorts {
		result.WriteString(fmt.Sprintf("localhost|localhost|%d", port))

		if i < len(s.discoveryPorts)-1 {
			result.WriteString(" ")
		}
	}

	result.WriteString("\n\r\n")

	return result.String()
}

func (s *fakeDiscoveryMemcacheServer) handleFakeDiscoveryMemcacheRequest(c net.Conn) {
	defer func() {
		_ = c.Close()
	}()

	reader := bufio.NewReader(c)

	_, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	//nolint:errcheck
	c.Write([]byte(s.getResponseToSend()))
}

func (s *fakeDiscoveryMemcacheServer) updateDiscoveryResponse(response string) {
	s.discoveryResponseMutex.Lock()
	defer s.discoveryResponseMutex.Unlock()

	s.discoveryConfigResponse = response
	s.discoveryConfigID = 0
	s.discoveryPorts = nil
}

func (s *fakeDiscoveryMemcacheServer) updateDiscoveryInformation(id int, ports []int) {
	s.discoveryResponseMutex.Lock()
	defer s.discoveryResponseMutex.Unlock()

	s.discoveryConfigID = id
	s.discoveryPorts = ports
	s.discoveryConfigResponse = ""
}

func TestNewDiscoveryClientInvalidPollingDuration(t *testing.T) {
	// Test that polling duration less than 1 second returns error
	_, err := NewDiscoveryClient("localhost:11211", 500*time.Millisecond)
	if !errors.Is(err, ErrInvalidPollingDuration) {
		t.Errorf("expected ErrInvalidPollingDuration, got %v", err)
	}

	_, err = NewDiscoveryClient("localhost:11211", 0)
	if !errors.Is(err, ErrInvalidPollingDuration) {
		t.Errorf("expected ErrInvalidPollingDuration for 0 duration, got %v", err)
	}
}

func TestFakeDiscoveryServer(t *testing.T) {
	ctx := context.Background()
	server := &fakeDiscoveryMemcacheServer{}

	err := server.start(ctx)
	if err != nil {
		t.Fatalf("failed to start fake server: %v", err)
	}
	//nolint:errcheck
	defer server.stop()

	// Test updating discovery information
	server.updateDiscoveryInformation(1, []int{11211, 11212})

	// Connect and verify response
	conn, err := net.Dial("tcp", server.currentAddress)
	if err != nil {
		t.Fatalf("failed to connect to fake server: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	// Send config get request
	_, err = conn.Write([]byte("config get cluster\r\n"))
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}

	// Read response
	reader := bufio.NewReader(conn)

	var response strings.Builder

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		response.WriteString(line)

		if strings.Contains(line, "localhost") {
			break
		}
	}

	if !strings.Contains(response.String(), "CONFIG cluster") {
		t.Errorf("unexpected response: %s", response.String())
	}

	if !strings.Contains(response.String(), "localhost|localhost|11211") {
		t.Errorf("expected localhost:11211 in response: %s", response.String())
	}
}

func TestFakeDiscoveryServerEmptyResponse(t *testing.T) {
	ctx := context.Background()
	server := &fakeDiscoveryMemcacheServer{}

	err := server.start(ctx)
	if err != nil {
		t.Fatalf("failed to start fake server: %v", err)
	}
	//nolint:errcheck
	defer server.stop()

	// Test with configID = 0 (should return END)
	server.updateDiscoveryInformation(0, nil)

	conn, err := net.Dial("tcp", server.currentAddress)
	if err != nil {
		t.Fatalf("failed to connect to fake server: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.Write([]byte("config get cluster\r\n"))
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}

	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if !strings.Contains(line, "END") {
		t.Errorf("expected END response, got: %s", line)
	}
}

func TestFakeDiscoveryServerCustomResponse(t *testing.T) {
	ctx := context.Background()
	server := &fakeDiscoveryMemcacheServer{}

	err := server.start(ctx)
	if err != nil {
		t.Fatalf("failed to start fake server: %v", err)
	}
	//nolint:errcheck
	defer server.stop()

	// Test with custom response
	customResponse := "CUSTOM RESPONSE\r\n"
	server.updateDiscoveryResponse(customResponse)

	conn, err := net.Dial("tcp", server.currentAddress)
	if err != nil {
		t.Fatalf("failed to connect to fake server: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.Write([]byte("config get cluster\r\n"))
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}

	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if line != customResponse {
		t.Errorf("expected custom response %q, got: %q", customResponse, line)
	}
}
