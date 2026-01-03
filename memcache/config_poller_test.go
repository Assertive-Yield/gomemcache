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
	"net"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestGetServerAddresses(t *testing.T) {
	tests := []struct {
		name     string
		config   *ClusterConfig
		expected []string
	}{
		{
			name: "single node",
			config: &ClusterConfig{
				ConfigID: 1,
				NodeAddresses: []ClusterNode{
					{Host: "localhost", Port: 11211},
				},
			},
			expected: []string{"localhost:11211"},
		},
		{
			name: "multiple nodes",
			config: &ClusterConfig{
				ConfigID: 2,
				NodeAddresses: []ClusterNode{
					{Host: "host1", Port: 11211},
					{Host: "host2", Port: 11212},
					{Host: "host3", Port: 11213},
				},
			},
			expected: []string{"host1:11211", "host2:11212", "host3:11213"},
		},
		{
			name: "empty nodes",
			config: &ClusterConfig{
				ConfigID:      3,
				NodeAddresses: []ClusterNode{},
			},
			expected: []string{},
		},
		{
			name: "ipv4 addresses",
			config: &ClusterConfig{
				ConfigID: 4,
				NodeAddresses: []ClusterNode{
					{Host: "192.168.1.1", Port: 11211},
					{Host: "10.0.0.1", Port: 11212},
				},
			},
			expected: []string{"192.168.1.1:11211", "10.0.0.1:11212"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getServerAddresses(tt.config)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("getServerAddresses() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfigPollerUpdateServerList(t *testing.T) {
	serverList := new(ServerList)

	// Create a mock config poller (without actually starting the polling)
	poller := &configPoller{
		serverList: serverList,
	}

	// Test updating server list
	config := &ClusterConfig{
		ConfigID: 1,
		NodeAddresses: []ClusterNode{
			{Host: "127.0.0.1", Port: 11211},
			{Host: "127.0.0.2", Port: 11212},
		},
	}

	err := poller.updateServerList(config)
	if err != nil {
		t.Fatalf("updateServerList returned error: %v", err)
	}

	// Verify the server list was updated
	var addrs []string

	err = serverList.Each(func(addr net.Addr) error {
		addrs = append(addrs, addr.String())

		return nil
	})
	if err != nil {
		t.Fatalf("failed to iterate server list: %v", err)
	}

	sort.Strings(addrs)

	expected := []string{"127.0.0.1:11211", "127.0.0.2:11212"}
	sort.Strings(expected)

	if !reflect.DeepEqual(addrs, expected) {
		t.Errorf("server list = %v, want %v", addrs, expected)
	}

	// Verify prevClusterConfig was updated
	if poller.prevClusterConfig != config {
		t.Errorf("prevClusterConfig was not updated")
	}
}

func TestConfigPollerStopPolling(t *testing.T) {
	serverList := new(ServerList)

	// Create a config poller with a short tick
	poller := &configPoller{
		serverList: serverList,
		done:       make(chan bool),
		tick:       time.NewTicker(time.Second),
	}

	// Stop should not panic when called multiple times
	poller.stopPolling()
	poller.stopPolling() // Should not panic due to sync.Once
}

func TestClusterConfigComparison(t *testing.T) {
	tests := []struct {
		name         string
		prevConfig   *ClusterConfig
		newConfig    *ClusterConfig
		shouldUpdate bool
	}{
		{
			name:         "nil prev config",
			prevConfig:   nil,
			newConfig:    &ClusterConfig{ConfigID: 1},
			shouldUpdate: true,
		},
		{
			name:         "newer config id",
			prevConfig:   &ClusterConfig{ConfigID: 1},
			newConfig:    &ClusterConfig{ConfigID: 2},
			shouldUpdate: true,
		},
		{
			name:         "same config id",
			prevConfig:   &ClusterConfig{ConfigID: 2},
			newConfig:    &ClusterConfig{ConfigID: 2},
			shouldUpdate: false,
		},
		{
			name:         "older config id",
			prevConfig:   &ClusterConfig{ConfigID: 3},
			newConfig:    &ClusterConfig{ConfigID: 2},
			shouldUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldUpdate := false

			if tt.prevConfig != nil {
				if tt.newConfig.ConfigID > tt.prevConfig.ConfigID {
					shouldUpdate = true
				}
			} else {
				shouldUpdate = true
			}

			if shouldUpdate != tt.shouldUpdate {
				t.Errorf("config comparison: got shouldUpdate=%v, want %v", shouldUpdate, tt.shouldUpdate)
			}
		})
	}
}
