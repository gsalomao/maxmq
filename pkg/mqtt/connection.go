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
	"errors"
	"io"
	"net"
	"time"

	"github.com/gsalomao/maxmq/pkg/logger"
	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
)

// ConnectionHandler is responsible for handle the connection that has been
// opened by the MQTT listener.
type ConnectionHandler interface {
	// Handle handles the opened TCP connection.
	Handle(tcpConn net.Conn)
}

// ConnectionManager manages the opened connections.
type ConnectionManager struct {
	log  *logger.Logger
	conf Configuration
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(
	cf Configuration,
	lg *logger.Logger,
) ConnectionManager {
	return ConnectionManager{
		conf: cf,
		log:  lg,
	}
}

// Handle handles the new opened TCP connection.
func (cm *ConnectionManager) Handle(tcpConn net.Conn) {
	defer cm.Close(tcpConn)

	conn := cm.newConnection(tcpConn)

	addr := conn.tcpConn.RemoteAddr().String()
	cm.log.Debug().Msg("MQTT Handling connection from " + addr)

	deadline := time.Now().
		Add(time.Duration(cm.conf.ConnectTimeout) * time.Second)

	for {
		err := conn.tcpConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().Msg("MQTT Failed to set read deadline: " +
				err.Error())
			break
		}

		cm.log.Trace().Msg("MQTT Waiting packet from " + addr)

		pkt, err := conn.reader.ReadPacket()
		if err != nil {
			if err == io.EOF {
				cm.log.Debug().Msg("MQTT Connection was closed by " + addr)
				break
			}

			if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
				cm.log.Debug().Msg("MQTT No CONNECT Packet received from " +
					addr)
				break
			}

			cm.log.Warn().Msg("MQTT Failed to read packet from " + addr +
				": " + err.Error())
			break
		}

		err = cm.processPacket(pkt, &conn)
		if err != nil {
			cm.log.Warn().Msg("MQTT Failed to process " + pkt.Type().String() +
				" Packet from " + addr + ": " + err.Error())
			break
		}

		// TODO: Update the deadline based on the IdleTimeout from configuration
	}
}

// Close closes the given TCP connection.
func (cm *ConnectionManager) Close(tcpConn net.Conn) {
	if tcp, ok := tcpConn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}

	addr := tcpConn.RemoteAddr().String()
	tcpConn.Close()
	cm.log.Debug().Msg("MQTT Closed connection with " + addr)
}

func (cm *ConnectionManager) processPacket(
	pkt packet.Packet,
	conn *connection,
) error {
	return errors.New("not implemented")
}

type connection struct {
	tcpConn net.Conn
	reader  packet.Reader
}

func (cm *ConnectionManager) newConnection(tcpConn net.Conn) connection {
	return connection{
		tcpConn: tcpConn,
		reader:  packet.NewReader(tcpConn, cm.conf.BufferSize),
	}
}
