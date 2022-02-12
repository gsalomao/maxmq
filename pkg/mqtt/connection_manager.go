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

	cm.log.Debug().
		Str("Address", conn.address).
		Msg("MQTT Handling connection")

	deadline := time.Now().
		Add(time.Duration(cm.conf.ConnectTimeout) * time.Second)

	for {
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().
				Msg("MQTT Failed to set read deadline: " + err.Error())
			break
		}

		cm.log.Trace().
			Str("Address", conn.address).
			Msg("MQTT Waiting packet")

		pkt, err := conn.reader.ReadPacket()
		if err != nil {
			if err == io.EOF {
				cm.log.Debug().
					Str("Address", conn.address).
					Msg("MQTT Connection was closed")
				break
			}

			if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
				cm.log.Debug().
					Str("Address", conn.address).
					Str("ClientID", string(conn.clientID)).
					Bool("Connected", conn.connected).
					Uint16("KeepAlive", conn.keepAlive).
					Msg("MQTT Timeout - No packet received")
				break
			}

			cm.log.Warn().
				Str("Address", conn.address).
				Msg("MQTT Failed to read packet: " + err.Error())

			if pktErr, ok := err.(packet.Error); ok {
				_ = cm.sendConnAck(&conn, pktErr.Code, false, nil)
			}
			break
		}

		cm.log.Debug().
			Str("Address", conn.address).
			Stringer("PacketType", pkt.Type()).
			Msg("MQTT Received packet")

		err = cm.processPacket(pkt, &conn)
		if err != nil {
			cm.log.Warn().
				Str("Address", conn.address).
				Stringer("PacketType", pkt.Type()).
				Msg("MQTT Failed to process packet: " + err.Error())
			break
		}

		// TODO: Use MaxKeepAlive from config to compute the next deadline
		if conn.keepAlive > 0 {
			timeout := float32(conn.keepAlive) * 1.5
			deadline = time.Now().Add(time.Duration(timeout) * time.Second)
		} else {
			// Zero value of time to disable the deadline
			deadline = time.Time{}
		}
	}
}

// Close closes the given connection.
func (cm *ConnectionManager) Close(conn Connection) {
	if tcp, ok := conn.netConn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}

	cm.log.Debug().
		Str("Address", conn.address).
		Msg("MQTT Closing connection")

	_ = conn.netConn.Close()
}

func (cm *ConnectionManager) processPacket(
	pkt packet.Packet,
	conn *Connection,
) error {
	if pkt.Type() == packet.CONNECT {
		connPkt, _ := pkt.(*packet.Connect)
		return cm.processPacketConnect(connPkt, conn)
	}

	return errors.New("invalid packet type: " + pkt.Type().String())
}

func (cm *ConnectionManager) processPacketConnect(
	pkt *packet.Connect,
	conn *Connection,
) error {
	cm.log.Trace().
		Str("Address", conn.address).
		Str("ClientID", string(pkt.ClientID)).
		Str("Version", pkt.Version.String()).
		Msg("MQTT Processing CONNECT Packet")

	conn.clientID = pkt.ClientID
	conn.version = pkt.Version
	conn.keepAlive = pkt.KeepAlive
	code := packet.ReturnCodeV3ConnectionAccepted

	err := cm.sendConnAck(conn, code, false, nil)
	if err != nil {
		return err
	}

	conn.connected = true
	cm.log.Debug().
		Str("Address", conn.address).
		Str("ClientID", string(pkt.ClientID)).
		Uint16("KeepAlive", conn.keepAlive).
		Msg("MQTT Client connected")

	return nil
}

func (cm *ConnectionManager) sendConnAck(
	conn *Connection,
	code packet.ReturnCode,
	sessionPresent bool,
	props *packet.Properties,
) error {
	pkt := packet.NewConnAck(conn.version, code, sessionPresent, props)

	cm.log.Trace().
		Str("Address", conn.address).
		Str("ClientID", string(conn.clientID)).
		Uint8("ReturnCode", uint8(code)).
		Str("Version", conn.version.String()).
		Msg("MQTT Sending CONNACK Packet")

	err := pkt.Pack(conn.netConn)
	if err != nil {
		cm.log.Error().
			Str("Address", conn.address).
			Str("ClientID", string(conn.clientID)).
			Uint8("ReturnCode", uint8(code)).
			Str("Version", conn.version.String()).
			Msg("MQTT Failed to send CONNACK Packet: " + err.Error())
	}

	return err
}
