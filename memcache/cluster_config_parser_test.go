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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// newMockReader creates a bufio.Reader for testing.
func newMockReader(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}

func prepareConfigResponse(discoveryID int, discoveryAddress [][]string) string {
	var temp strings.Builder

	temp.WriteString("CONFIG cluster 0 80\r\n")
	temp.WriteString(strconv.Itoa(discoveryID))
	temp.WriteString("\r\n")

	for i, address := range discoveryAddress {
		temp.WriteString(fmt.Sprintf("%s|%s|%s", address[0], address[0], address[1]))

		if i < len(discoveryAddress)-1 {
			temp.WriteString(" ")
		}
	}

	temp.WriteString("\n\r\n")

	return temp.String()
}

func buildClusterConfig(discoveryID int, discoveryAddress [][]string) *ClusterConfig {
	cc := &ClusterConfig{ConfigID: int64(discoveryID)}

	cc.NodeAddresses = make([]ClusterNode, len(discoveryAddress))

	for i, address := range discoveryAddress {
		port, _ := strconv.ParseInt(address[1], 10, 64)
		cc.NodeAddresses[i] = ClusterNode{Host: address[0], Port: port}
	}

	return cc
}

func TestGoodClusterConfigs(t *testing.T) {
	configTests := []struct {
		discoveryID        int
		discoveryAddresses [][]string
	}{
		{2, [][]string{{"localhost", "112233"}}},
		{1000, [][]string{{"localhost", "112233"}, {"127.0.0.4", "123435"}}},
		{50, [][]string{{"localhost", "112233"}, {"127.0.0.4", "123435"}, {"127.0.0.5", "123"}}},
	}
	for _, tt := range configTests {
		config := prepareConfigResponse(tt.discoveryID, tt.discoveryAddresses)
		want := buildClusterConfig(tt.discoveryID, tt.discoveryAddresses)
		reader := newMockReader(config)
		got := &ClusterConfig{}

		f := func(cb *ClusterConfig) {
			got = cb
		}
		err := parseConfigGetResponse(reader, f)
		if err != nil {
			t.Errorf("parseConfigGetResponse(%v) returned error: %v", config, err)
		}

		if !reflect.DeepEqual(want, got) {
			t.Errorf("parseConfigGetResponse(%v) got: %v, want: %v", config, got, want)
		}
	}
}

func TestBrokenClusterConfigs(t *testing.T) {
	brokenConfigTests := []struct {
		name   string
		config string
	}{
		{"missing config id", "CONFIG cluster 0 80\r\n"},
		{"invalid config id", "CONFIG cluster 0 80\r\nabc\r\nlocalhost|localhost|11211\n\r\n"},
		{"invalid port", "CONFIG cluster 0 80\r\n1\r\nlocalhost|localhost|notaport\n\r\n"},
		{"invalid node format", "CONFIG cluster 0 80\r\n1\r\nlocalhost\n\r\n"},
	}
	for _, tt := range brokenConfigTests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newMockReader(tt.config)

			var got *ClusterConfig

			f := func(cb *ClusterConfig) {
				got = cb
			}

			err := parseConfigGetResponse(reader, f)
			if err == nil && got != nil {
				t.Errorf("parseConfigGetResponse(%v) expected error but got nil with config: %v", tt.config, got)
			}
		})
	}
}

func TestEmptyConfigResponse(t *testing.T) {
	// Test END response (no config found)
	config := "END\r\n"
	reader := newMockReader(config)
	called := false

	f := func(cb *ClusterConfig) {
		called = true
	}
	err := parseConfigGetResponse(reader, f)
	if err != nil {
		t.Errorf("parseConfigGetResponse(%v) returned error: %v", config, err)
	}

	if called {
		t.Errorf("callback should not have been called for END response")
	}
}

func TestEmptyNodesListConfig(t *testing.T) {
	// Test config with empty nodes list
	config := "CONFIG cluster 0 10\r\n42\r\n\n\r\n"
	reader := newMockReader(config)

	var got *ClusterConfig

	f := func(cb *ClusterConfig) {
		got = cb
	}
	err := parseConfigGetResponse(reader, f)
	if err != nil {
		t.Errorf("parseConfigGetResponse(%v) returned error: %v", config, err)
	}

	if got == nil {
		t.Fatal("expected non-nil config")
	}

	if got.ConfigID != 42 {
		t.Errorf("expected ConfigID 42, got %d", got.ConfigID)
	}

	if len(got.NodeAddresses) != 0 {
		t.Errorf("expected empty NodeAddresses, got %v", got.NodeAddresses)
	}
}

func TestMultipleNodesConfig(t *testing.T) {
	// Test config with multiple nodes
	config := "CONFIG cluster 0 100\r\n123\r\nhost1|host1|11211 host2|host2|11212 host3|host3|11213\n\r\n"
	reader := newMockReader(config)

	var got *ClusterConfig

	f := func(cb *ClusterConfig) {
		got = cb
	}
	err := parseConfigGetResponse(reader, f)
	if err != nil {
		t.Errorf("parseConfigGetResponse returned error: %v", err)
	}

	if got == nil {
		t.Fatal("expected non-nil config")
	}

	if got.ConfigID != 123 {
		t.Errorf("expected ConfigID 123, got %d", got.ConfigID)
	}

	if len(got.NodeAddresses) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(got.NodeAddresses))
	}

	expectedNodes := []ClusterNode{
		{Host: "host1", Port: 11211},
		{Host: "host2", Port: 11212},
		{Host: "host3", Port: 11213},
	}
	for i, node := range got.NodeAddresses {
		if node.Host != expectedNodes[i].Host || node.Port != expectedNodes[i].Port {
			t.Errorf("node %d: expected %v, got %v", i, expectedNodes[i], node)
		}
	}
}

func BenchmarkParseConfigGetResponse(b *testing.B) {
	config := prepareConfigResponse(100, [][]string{
		{"host1", "11211"},
		{"host2", "11212"},
		{"host3", "11213"},
	})

	b.ReportAllocs()

	for b.Loop() {
		reader := newMockReader(config)
		//nolint:errcheck
		parseConfigGetResponse(reader, func(cc *ClusterConfig) {})
	}
}
