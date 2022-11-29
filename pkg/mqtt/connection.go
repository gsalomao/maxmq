// Copyright 2022 The MaxMQ Authors
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
	"time"

	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
)

type connection struct {
	netConn    net.Conn
	version    packet.MQTTVersion
	clientID   ClientID
	timeout    int
	connected  bool
	hasSession bool
}

func (c *connection) nextConnectionDeadline() time.Time {
	if c.timeout > 0 {
		timeout := math.Ceil(float64(c.timeout) * 1.5)
		return time.Now().Add(time.Duration(timeout) * time.Second)
	}

	// Zero value of time to disable the timeout
	return time.Time{}
}
