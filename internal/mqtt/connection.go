// Copyright 2022-2023 The MaxMQ Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type connectionState uint8

const (
	stateNew connectionState = iota
	stateActive
	stateIdle
	stateClosing
	stateClosed
)

type connection struct {
	netConn    net.Conn
	version    packet.Version
	clientID   packet.ClientID
	currState  atomic.Uint64
	listener   string
	closeOnce  sync.Once
	timeout    int
	hasSession bool
}

func (c *connection) nextDeadline() time.Time {
	if c.timeout > 0 {
		timeout := math.Ceil(float64(c.timeout) * 1.5)
		return time.Now().Add(time.Duration(timeout) * time.Second)
	}

	// Zero value of time to disable the timeout
	return time.Time{}
}

func (c *connection) state() connectionState {
	st := c.currState.Load()
	return connectionState(st & 0xFF)
}

func (c *connection) setState(s connectionState) {
	c.currState.Store(uint64(s))
}

func (c *connection) connected() bool {
	return c.state() < stateClosing
}

func (c *connection) close(force bool) {
	c.closeOnce.Do(func() {
		c.setState(stateClosing)

		if tcp, ok := c.netConn.(*net.TCPConn); ok && force {
			_ = tcp.SetLinger(0)
		}

		_ = c.netConn.Close()
		c.setState(stateClosed)
	})
}
