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

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
)

// ConnectionManager implements the ConnectionHandler interface.
type ConnectionManager struct {
	log            *logger.Logger
	conf           Configuration
	userProperties []packet.UserProperty
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(
	cf Configuration,
	lg *logger.Logger,
) ConnectionManager {
	cf.BufferSize = bufferSizeOrDefault(cf.BufferSize)
	cf.MaxPacketSize = maxPacketSizeOrDefault(cf.MaxPacketSize)
	cf.ConnectTimeout = connectTimeoutOrDefault(cf.ConnectTimeout)
	cf.MaximumQoS = maximumQosOrDefault(cf.MaximumQoS)
	cf.MaxTopicAlias = maxTopicAliasOrDefault(cf.MaxTopicAlias)
	cf.MaxInflightMessages = maxInflightMsgOrDefault(cf.MaxInflightMessages)

	userProps := make([]packet.UserProperty, 0, len(cf.UserProperties))
	for k, v := range cf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	lg.Trace().
		Int("BufferSize", cf.BufferSize).
		Int("ConnectTimeout", cf.ConnectTimeout).
		Int("MaximumQoS", cf.MaximumQoS).
		Int("MaxInflightMessages", cf.MaxInflightMessages).
		Int("MaxPacketSize", cf.MaxPacketSize).
		Uint32("MaxSessionExpiryInterval", cf.MaxSessionExpiryInterval).
		Int("MaxTopicAlias", cf.MaxTopicAlias).
		Bool("RetainAvailable", cf.RetainAvailable).
		Bool("WildcardSubscriptionAvailable", cf.WildcardSubscriptionAvailable).
		Bool("SubscriptionIDAvailable", cf.SubscriptionIDAvailable).
		Bool("SharedSubscriptionAvailable", cf.SharedSubscriptionAvailable).
		Msg("MQTT Creating Connection Manager")

	return ConnectionManager{
		conf:           cf,
		log:            lg,
		userProperties: userProps,
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
				connAck := newConnAck(nil, pktErr.Code, false, cm.conf)
				_ = cm.sendConnAck(&conn, connAck)
			}
			break
		}

		cm.log.Debug().
			Str("Address", conn.address).
			Stringer("PacketType", pkt.Type()).
			Msg("MQTT Received packet")

		err = cm.handlePacket(pkt, &conn)
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

func (cm *ConnectionManager) handlePacket(
	pkt packet.Packet,
	conn *Connection,
) error {
	if pkt.Type() == packet.CONNECT {
		connPkt, _ := pkt.(*packet.Connect)
		return cm.handlePacketConnect(connPkt, conn)
	}

	return errors.New("invalid packet type: " + pkt.Type().String())
}

func (cm *ConnectionManager) handlePacketConnect(
	connPkt *packet.Connect,
	conn *Connection,
) error {
	cm.log.Trace().
		Str("Address", conn.address).
		Str("ClientID", string(connPkt.ClientID)).
		Str("Version", connPkt.Version.String()).
		Uint16("KeepAlive", connPkt.KeepAlive).
		Msg("MQTT Handling CONNECT Packet")

	conn.clientID = connPkt.ClientID
	conn.version = connPkt.Version
	conn.timeout = connPkt.KeepAlive

	if connPkt.Version != packet.MQTT50 && cm.conf.MaxKeepAlive > 0 {
		ka := int(connPkt.KeepAlive)
		if ka == 0 || ka > cm.conf.MaxKeepAlive {
			// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
			// clients what Keep Alive value they should use. If an MQTT
			// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
			// MaxKeepAlive, the CONNACK Packet is sent with the reason code
			// "identifier rejected".
			code := packet.ReturnCodeV3IdentifierRejected
			connAckPkt := newConnAck(connPkt, code, false, cm.conf)
			_ = cm.sendConnAck(conn, connAckPkt)
			return errors.New("keep alive exceeded")
		}
	}

	code := packet.ReturnCodeV3ConnectionAccepted
	connAck := newConnAck(connPkt, code, false, cm.conf)
	if connAck.Properties != nil {
		connAck.Properties.UserProperties = cm.userProperties
	}

	err := cm.sendConnAck(conn, connAck)
	if err != nil {
		return err
	}

	conn.connected = true
	if connAck.Properties != nil && connAck.Properties.ServerKeepAlive != nil {
		conn.timeout = *connAck.Properties.ServerKeepAlive
	}

	cm.log.Debug().
		Str("Address", conn.address).
		Str("ClientID", string(conn.clientID)).
		Uint16("Timeout", conn.timeout).
		Msg("MQTT Client connected")

	return nil
}

func (cm *ConnectionManager) sendConnAck(
	conn *Connection,
	pkt packet.ConnAck,
) error {
	cm.log.Trace().
		Str("Address", conn.address).
		Str("ClientID", string(conn.clientID)).
		Uint8("ReturnCode", uint8(pkt.ReturnCode)).
		Str("Version", conn.version.String()).
		Msg("MQTT Sending CONNACK Packet")

	err := conn.writer.WritePacket(&pkt)
	if err != nil {
		cm.log.Error().
			Str("Address", conn.address).
			Str("ClientID", string(conn.clientID)).
			Uint8("ReturnCode", uint8(pkt.ReturnCode)).
			Str("Version", conn.version.String()).
			Msg("MQTT Failed to send CONNACK Packet: " + err.Error())
	}

	return err
}
