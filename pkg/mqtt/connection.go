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

	"github.com/gsalomao/maxmq/pkg/logger"
)

// ConnectionHandler is responsible to handle the connection that has been
// opened by the MQTT listener.
type ConnectionHandler interface {
	// Handle handles the new opened TCP connection.
	Handle(conn net.Conn)
}

// ConnectionManager manages the opened connections.
type ConnectionManager struct {
	log *logger.Logger
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(l *logger.Logger) ConnectionManager {
	return ConnectionManager{
		log: l,
	}
}

// Handle handles the new opened TCP connection.
func (cm *ConnectionManager) Handle(conn net.Conn) {
	defer cm.Close(conn)

	cm.log.Debug().Msg("Handling connection from " +
		conn.RemoteAddr().String())
}

// Close closes the given TCP connection.
func (cm *ConnectionManager) Close(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}

	addr := conn.RemoteAddr().String()
	conn.Close()
	cm.log.Debug().Msg("Connection with " + addr + " was closed")
}
