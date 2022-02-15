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

const (
	defaultBufferSize     = 1024
	minBufferSize         = 1
	maxBufferSize         = 65536
	minPacketSize         = 20
	maxPacketSize         = 268435456
	defaultConnectTimeout = 5
	minConnectTimeout     = 1
	minQoS                = 0
	maxQoS                = 2
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
	if cf.BufferSize < minBufferSize || cf.BufferSize > maxBufferSize {
		cf.BufferSize = defaultBufferSize
	}

	if cf.MaxPacketSize < minPacketSize || cf.MaxPacketSize > maxPacketSize {
		cf.MaxPacketSize = maxPacketSize
	}

	if cf.ConnectTimeout < minConnectTimeout {
		cf.ConnectTimeout = defaultConnectTimeout
	}

	if cf.MaximumQoS < minQoS || cf.MaximumQoS > maxQoS {
		cf.MaximumQoS = maxQoS
	}

	lg.Trace().
		Int("BufferSize", cf.BufferSize).
		Int("ConnectTimeout", cf.ConnectTimeout).
		Int("MaximumQoS", cf.MaximumQoS).
		Int("MaxPacketSize", cf.MaxPacketSize).
		Bool("RetainAvailable", cf.RetainAvailable).
		Msg("MQTT Creating Connection Manager")

	return ConnectionManager{
		conf: cf,
		log:  lg,
	}
}

// NewConnection creates a new Connection.
func (cm *ConnectionManager) NewConnection(nc net.Conn) Connection {
	rdOpts := packet.ReaderOptions{
		BufferSize:    cm.conf.BufferSize,
		MaxPacketSize: cm.conf.MaxPacketSize,
	}

	return Connection{
		netConn: nc,
		reader:  packet.NewReader(nc, rdOpts),
		writer:  packet.NewWriter(nc, cm.conf.BufferSize),
		address: nc.RemoteAddr().String(),
		timeout: uint16(cm.conf.ConnectTimeout),
	}
}

// Handle handles the Connection.
func (cm *ConnectionManager) Handle(conn Connection) {
	defer cm.Close(conn)

	cm.log.Debug().
		Str("Address", conn.address).
		Uint16("Timeout", conn.timeout).
		Msg("MQTT Handling connection")

	deadline := time.Now().Add(time.Duration(conn.timeout) * time.Second)

	for {
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().
				Msg("MQTT Failed to set read deadline: " + err.Error())
			break
		}

		cm.log.Trace().
			Str("Address", conn.address).
			Uint16("Timeout", conn.timeout).
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
					Uint16("Timeout", conn.timeout).
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

		if conn.timeout > 0 {
			timeout := float32(conn.timeout) * 1.5
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
		Uint16("KeepAlive", pkt.KeepAlive).
		Msg("MQTT Processing CONNECT Packet")

	conn.clientID = pkt.ClientID
	conn.version = pkt.Version
	conn.timeout = pkt.KeepAlive

	if pkt.Version != packet.MQTT50 && cm.conf.MaxKeepAlive > 0 {
		keepAlive := int(pkt.KeepAlive)
		if keepAlive == 0 || keepAlive > cm.conf.MaxKeepAlive {
			// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
			// clients what Keep Alive value they should use. If an MQTT
			// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
			// MaxKeepAlive, the CONNACK Packet is sent with the reason code
			// "identifier rejected".
			code := packet.ReturnCodeV3IdentifierRejected
			_ = cm.sendConnAck(conn, code, false, nil)
			return errors.New("keep alive exceeded")
		}
	}

	var props *packet.Properties
	if pkt.Version == packet.MQTT50 {
		props = cm.newConnAckProperties(pkt, conn)
	}

	code := packet.ReturnCodeV3ConnectionAccepted
	err := cm.sendConnAck(conn, code, false, props)
	if err != nil {
		return err
	}

	conn.connected = true
	cm.log.Debug().
		Str("Address", conn.address).
		Str("ClientID", string(conn.clientID)).
		Uint16("Timeout", conn.timeout).
		Msg("MQTT Client connected")

	return nil
}

func (cm *ConnectionManager) newConnAckProperties(
	pkt *packet.Connect,
	conn *Connection,
) *packet.Properties {
	pr := packet.Properties{}
	hasProps := false

	keepAlive := int(pkt.KeepAlive)
	if cm.conf.MaxKeepAlive > 0 && keepAlive > cm.conf.MaxKeepAlive {
		conn.timeout = uint16(cm.conf.MaxKeepAlive)

		pr.ServerKeepAlive = new(uint16)
		*pr.ServerKeepAlive = conn.timeout
		hasProps = true
	}

	if cm.conf.MaxPacketSize != maxPacketSize {
		pr.MaximumPacketSize = new(uint32)
		*pr.MaximumPacketSize = uint32(cm.conf.MaxPacketSize)
		hasProps = true
	}

	if cm.conf.MaximumQoS < maxQoS {
		pr.MaximumQoS = new(byte)
		*pr.MaximumQoS = byte(cm.conf.MaximumQoS)
		hasProps = true
	}

	if !cm.conf.RetainAvailable {
		ra := byte(1)
		if !cm.conf.RetainAvailable {
			ra = 0
		}

		pr.RetainAvailable = new(byte)
		*pr.RetainAvailable = ra
		hasProps = true
	}

	if !hasProps {
		return nil
	}

	return &pr
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

	err := conn.writer.WritePacket(&pkt)
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
