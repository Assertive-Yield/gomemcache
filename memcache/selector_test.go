/*
Copyright 2014 The gomemcache AUTHORS

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
	"errors"
	"net"
	"testing"
)

func BenchmarkPickServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_Single(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()

	var ss ServerList

	err := ss.SetServers(servers...)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer([]byte("some key")); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPickAnyServer(t *testing.T) {
	pickServerTests := []struct {
		serverList            []string
		expectedServersPicked int
	}{
		{[]string{"127.0.0.1:1234"}, 1},
		{[]string{"127.0.0.1:1234", "127.0.0.1:1235", "127.0.0.1:1236"}, 2},
	}
	for _, tt := range pickServerTests {
		var ss ServerList

		err := ss.SetServers(tt.serverList...)
		if err != nil {
			t.Fatalf("SetServers returned error: %v", err)
		}

		serverCounter := make(map[string]int)

		for i := 0; i < 1000; i++ {
			var (
				addr net.Addr
				err  error
			)

			if addr, err = ss.PickAnyServer(); err != nil {
				t.Errorf("PickAnyServer(%v) failed due to %v", tt.serverList, err)
			}

			serverCounter[addr.String()]++
		}
		// Verify that server counter contains at least expected number of values
		if len(serverCounter) < tt.expectedServersPicked {
			t.Errorf("failed to randomize server list (%v), serverCounter (%v). got:%v, want at least:%v",
				tt.serverList,
				serverCounter,
				len(serverCounter),
				tt.expectedServersPicked,
			)
		}
	}
}

func TestPickAnyServerThrows(t *testing.T) {
	var ss ServerList
	if _, err := ss.PickAnyServer(); err != nil && !errors.Is(err, ErrNoServers) {
		t.Errorf("expected error with no servers, got:%v", err)
	}
}

func BenchmarkPickAnyServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickAnyServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickAnyServer_Single(b *testing.B) {
	benchPickAnyServer(b, "127.0.0.1:1234")
}

func benchPickAnyServer(b *testing.B, servers ...string) {
	b.ReportAllocs()

	var ss ServerList

	err := ss.SetServers(servers...)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := ss.PickAnyServer(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSetServers(t *testing.T) {
	var ss ServerList

	err := ss.SetServers("127.0.0.1:11211", "127.0.0.2:11211")
	if err != nil {
		t.Fatalf("SetServers returned error: %v", err)
	}

	count := 0

	err = ss.Each(func(addr net.Addr) error {
		count++

		return nil
	})
	if err != nil {
		t.Fatalf("Each returned error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 servers, got %d", count)
	}
}

func TestSetServersWithUnixSocket(t *testing.T) {
	var ss ServerList

	err := ss.SetServers("/tmp/memcached.sock")
	if err != nil {
		t.Fatalf("SetServers returned error: %v", err)
	}

	count := 0

	err = ss.Each(func(addr net.Addr) error {
		count++

		if addr.Network() != "unix" {
			t.Errorf("expected unix network, got %s", addr.Network())
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Each returned error: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 server, got %d", count)
	}
}

func TestPickServerConsistency(t *testing.T) {
	var ss ServerList

	err := ss.SetServers("127.0.0.1:11211", "127.0.0.2:11211", "127.0.0.3:11211")
	if err != nil {
		t.Fatalf("SetServers returned error: %v", err)
	}

	key := []byte("test-key")

	addr1, err := ss.PickServer(key)
	if err != nil {
		t.Fatalf("PickServer returned error: %v", err)
	}

	// Same key should always return the same server
	for i := 0; i < 100; i++ {
		addr2, err := ss.PickServer(key)
		if err != nil {
			t.Fatalf("PickServer returned error: %v", err)
		}

		if addr1.String() != addr2.String() {
			t.Errorf("PickServer returned different servers for same key: %s vs %s", addr1.String(), addr2.String())
		}
	}
}
