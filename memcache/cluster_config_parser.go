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
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

var (
	configKeywordBytes = []byte("CONFIG")
	endBytes           = []byte("END")
)

var ErrNoConfig = errors.New("memcache: no cluster config found")

var ErrInvalidConfig = errors.New("memcache: invalid cluster config found")

// ClusterNode represents address of a memcached node.
type ClusterNode struct {
	Host string
	Port int64
}

// ClusterConfig represents cluster configuration which contains nodes and version.
type ClusterConfig struct {
	// ConfigID is the monotonically increasing identifier for the config information
	ConfigID int64

	// NodeAddresses are array of ClusterNode which contain address of a memcache node.
	NodeAddresses []ClusterNode
}

// parseConfigGetResponse reads a CONFIG GET response using bufio.Reader.
func parseConfigGetResponse(r *bufio.Reader, cb func(*ClusterConfig)) error {
	var clusterConfig ClusterConfig
	// Reset the config for reuse
	clusterConfig.ConfigID = 0
	clusterConfig.NodeAddresses = clusterConfig.NodeAddresses[:0]

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}

		line = bytes.TrimSpace(line)
		// Skip empty line
		if len(line) == 0 {
			continue
		}
		// CONFIG keyword line is expected
		if bytes.Contains(line, configKeywordBytes) {
			// Read config ID line
			configIDLine, err := r.ReadBytes('\n')
			if err != nil {
				return ErrInvalidConfig
			}

			configIDLine = bytes.TrimSpace(configIDLine)

			configID, err := strconv.ParseInt(string(configIDLine), 10, 64)
			if err != nil {
				return fmt.Errorf("memcache: failed to parse config id: %w", err)
			}

			clusterConfig.ConfigID = configID

			// Read nodes line
			nodesLine, err := r.ReadBytes('\n')
			if err != nil {
				return ErrInvalidConfig
			}

			nodesLine = bytes.TrimSpace(nodesLine)

			if len(nodesLine) == 0 {
				cb(&clusterConfig)
				return nil
			}

			// Parse nodes
			nodesCopy := nodesLine

			for len(nodesCopy) > 0 {
				spaceIdx := bytes.IndexByte(nodesCopy, ' ')

				var node []byte

				if spaceIdx == -1 {
					node = nodesCopy
					nodesCopy = nil
				} else {
					node = nodesCopy[:spaceIdx]
					nodesCopy = nodesCopy[spaceIdx+1:]
				}

				if len(node) == 0 {
					continue
				}

				pipe1 := bytes.IndexByte(node, '|')
				if pipe1 == -1 {
					return fmt.Errorf("memcache: invalid node format: %s", node)
				}

				host := node[:pipe1]

				remaining := node[pipe1+1:]

				pipe2 := bytes.IndexByte(remaining, '|')
				if pipe2 == -1 {
					return fmt.Errorf("memcache: invalid node format: %s", node)
				}

				portBytes := remaining[pipe2+1:]

				port, err := strconv.ParseInt(string(portBytes), 10, 64)
				if err != nil {
					return fmt.Errorf("memcache: failed to parse port for node %s: %w", node, err)
				}

				clusterConfig.NodeAddresses = append(clusterConfig.NodeAddresses, ClusterNode{
					Host: string(host),
					Port: port,
				})
			}

			cb(&clusterConfig)

			return nil
		}
		// Check for END response (no config found)
		if bytes.Equal(line, endBytes) {
			return nil
		}
	}
}
