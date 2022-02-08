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

// ConnectionManager implements the ConnectionHandler interface.
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

// NewConnection creates a new Connection.
func (cm *ConnectionManager) NewConnection(nc net.Conn) Connection {
	opts := packet.ReaderOptions{
		BufferSize:    cm.conf.BufferSize,
		MaxPacketSize: cm.conf.MaxPacketSize,
	}

	return Connection{
		netConn: nc,
		reader:  packet.NewReader(nc, opts),
		address: nc.RemoteAddr().String(),
	}
}

// Handle handles the Connection.
func (cm *ConnectionManager) Handle(conn Connection) {
	defer cm.Close(conn)

	cm.log.Debug().Str("Address", conn.address).Msg("MQTT Handling connection")

	deadline := time.Now().
		Add(time.Duration(cm.conf.ConnectTimeout) * time.Second)

	for {
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().Msg("MQTT Failed to set read deadline: " +
				err.Error())
			break
		}

		cm.log.Trace().Str("Address", conn.address).Msg("MQTT Waiting packet")

		pkt, err := conn.reader.ReadPacket()
		if err != nil {
			if err == io.EOF {
				cm.log.Debug().Str("Address", conn.address).
					Msg("MQTT Connection was closed")
				break
			}

			if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
				cm.log.Debug().Str("Address", conn.address).
					Msg("MQTT No CONNECT Packet received")
				break
			}

			cm.log.Warn().Str("Address", conn.address).
				Msg("MQTT Failed to read packet: " + err.Error())
			break
		}

		err = cm.processPacket(pkt, &conn)
		if err != nil {
			cm.log.Warn().Str("Address", conn.address).
				Msg("MQTT Failed to process " + pkt.Type().String() +
					" Packet: " + err.Error())
			break
		}

		// TODO: Update the deadline based on the IdleTimeout from configuration
	}
}

// Close closes the given connection.
func (cm *ConnectionManager) Close(conn Connection) {
	if tcp, ok := conn.netConn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}

	cm.log.Debug().Str("Address", conn.address).Msg("MQTT Closing connection")
	conn.netConn.Close()
}

func (cm *ConnectionManager) processPacket(
	pkt packet.Packet,
	conn *Connection,
) error {
	if pkt.Type() == packet.CONNECT {
		connPkt, _ := pkt.(*packet.PacketConnect)
		cm.log.Trace().Str("ClientID", string(connPkt.ClientID)).
			Str("Address", conn.address).
			Msg("MQTT Processing CONNECT Packet")
	}

	return errors.New("not implemented")
}
