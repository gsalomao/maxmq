/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mqtt

import (
	"net"

	"github.com/gsalomao/maxmq/mqtt/packet"
)

// Connection represents a network connection.
type Connection struct {
	netConn   net.Conn
	reader    packet.Reader
	writer    packet.Writer
	address   string
	connected bool
	timeout   uint16
	clientID  []byte
	version   packet.MQTTVersion
}

// ConnectionHandler is responsible for handle connections.
type ConnectionHandler interface {
	// NewConnection creates a new Connection.
	NewConnection(nc net.Conn) Connection

	// Handle handles the Connection.
	Handle(conn Connection)
}
