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
	"math"
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
	cf.MaxClientIDLen = maxClientIDLenOrDefault(cf.MaxClientIDLen)

	userProps := make([]packet.UserProperty, 0, len(cf.UserProperties))
	for k, v := range cf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	lg.Trace().
		Bool("AllowEmptyClientID", cf.AllowEmptyClientID).
		Int("BufferSize", cf.BufferSize).
		Int("ConnectTimeout", cf.ConnectTimeout).
		Int("MaximumQoS", cf.MaximumQoS).
		Int("MaxClientIDLen", cf.MaxClientIDLen).
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
				connAck := newConnAck(&conn, pktErr.Code, false, cm.conf, nil)
				_ = cm.sendPacket(&conn, &connAck)
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
				Str("ClientID", string(conn.clientID)).
				Stringer("PacketType", pkt.Type()).
				Msg("MQTT Failed to process packet: " + err.Error())
			break
		}

		if conn.timeout > 0 {
			timeout := math.Ceil(float64(conn.timeout) * 1.5)
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
	if !conn.connected && pkt.Type() != packet.CONNECT {
		return errors.New("unexpected packet type")
	}

	switch pkt.Type() {
	case packet.CONNECT:
		connPkt, _ := pkt.(*packet.Connect)
		return cm.handlePacketConnect(connPkt, conn)
	case packet.PINGREQ:
		pingResp := packet.PingResp{}
		return cm.sendPacket(conn, &pingResp)
	default:
		return errors.New("invalid packet type: " + pkt.Type().String())
	}
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

	if err := cm.checkKeepAlive(connPkt); err != nil {
		// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
		// clients what Keep Alive value they should use. If an MQTT
		// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
		// MaxKeepAlive, the CONNACK Packet is sent with the reason code
		// "identifier rejected".
		code := packet.ReturnCodeV3IdentifierRejected
		connAckPkt := newConnAck(conn, code, false, cm.conf, nil)
		_ = cm.sendPacket(conn, &connAckPkt)
		return err
	}

	if err := cm.checkClientID(connPkt); err != nil {
		connAckPkt := newConnAck(conn, err.Code, false, cm.conf, nil)
		_ = cm.sendPacket(conn, &connAckPkt)
		return err
	}

	code := packet.ReturnCodeV3ConnectionAccepted
	connAck := newConnAck(conn, code, false, cm.conf, connPkt.Properties)

	if len(connPkt.ClientID) == 0 {
		conn.clientID = generateClientID(cm.conf.ClientIDPrefix)

		if conn.version == packet.MQTT50 {
			connAck.Properties.AssignedClientID = conn.clientID
		}
	}

	if connAck.Properties != nil {
		connAck.Properties.UserProperties = cm.userProperties
	}

	if err := cm.sendPacket(conn, &connAck); err != nil {
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

func (cm *ConnectionManager) checkKeepAlive(pkt *packet.Connect) error {
	// For V5.0 clients, the Keep Alive sent in the CONNECT Packet can be
	// overwritten in the CONNACK Packet.
	if pkt.Version == packet.MQTT50 || cm.conf.MaxKeepAlive == 0 {
		return nil
	}

	keepAlive := int(pkt.KeepAlive)
	if keepAlive == 0 || keepAlive > cm.conf.MaxKeepAlive {
		return errors.New("keep alive exceeded")
	}

	return nil
}

func (cm *ConnectionManager) checkClientID(pkt *packet.Connect) *packet.Error {
	cIDLen := len(pkt.ClientID)

	if !cm.conf.AllowEmptyClientID && cIDLen == 0 {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}

		return packet.ErrV3IdentifierRejected
	}

	if pkt.Version == packet.MQTT31 && cIDLen > 23 {
		return packet.ErrV3IdentifierRejected
	}

	if cIDLen > cm.conf.MaxClientIDLen {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}

		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (cm *ConnectionManager) sendPacket(
	conn *Connection,
	pkt packet.Packet,
) error {
	cm.log.Trace().
		Str("Address", conn.address).
		Str("ClientID", string(conn.clientID)).
		Stringer("PacketType", pkt.Type()).
		Str("Version", conn.version.String()).
		Msg("MQTT Sending packet")

	err := conn.writer.WritePacket(pkt)
	if err != nil {
		cm.log.Error().
			Str("Address", conn.address).
			Str("ClientID", string(conn.clientID)).
			Stringer("PacketType", pkt.Type()).
			Str("Version", conn.version.String()).
			Msg("MQTT Failed to send packet: " + err.Error())
	}

	return err
}
