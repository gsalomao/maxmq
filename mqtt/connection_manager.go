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
	"errors"
	"io"
	"net"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"go.uber.org/multierr"
)

// ConnectionManager implements the ConnectionHandler interface.
type ConnectionManager struct {
	conf           Configuration
	log            *logger.Logger
	metrics        *metrics
	userProperties []packet.UserProperty
	reader         packet.Reader
	writer         packet.Writer
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

	userProps := make([]packet.UserProperty, len(cf.UserProperties))

	i := 0
	for k, v := range cf.UserProperties {
		userProps[i] = packet.UserProperty{Key: []byte(k), Value: []byte(v)}
		i++
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

	rdOpts := packet.ReaderOptions{
		BufferSize:    cf.BufferSize,
		MaxPacketSize: cf.MaxPacketSize,
	}

	return ConnectionManager{
		conf:           cf,
		log:            lg,
		metrics:        newMetrics(cf.MetricsEnabled, lg),
		userProperties: userProps,
		reader:         packet.NewReader(rdOpts),
		writer:         packet.NewWriter(cf.BufferSize),
	}
}

// NewConnection creates a new Connection.
func (cm *ConnectionManager) NewConnection(nc net.Conn) Connection {
	return Connection{
		netConn: nc,
		timeout: uint16(cm.conf.ConnectTimeout),
	}
}

// Handle handles the Connection.
func (cm *ConnectionManager) Handle(conn Connection) {
	defer cm.Close(&conn, true)
	cm.metrics.recordConnection()

	cm.log.Debug().
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
			Uint16("Timeout", conn.timeout).
			Msg("MQTT Waiting packet")

		pkt, err := cm.reader.ReadPacket(conn.netConn)
		if err != nil {
			if err == io.EOF {
				cm.log.Debug().Msg("MQTT Connection was closed")
				break
			}

			if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
				cm.log.Debug().
					Bytes("ClientID", conn.clientID).
					Bool("Connected", conn.connected).
					Uint16("Timeout", conn.timeout).
					Msg("MQTT Timeout - No packet received")
				break
			}

			cm.log.Info().Msg("MQTT Failed to read packet: " + err.Error())

			if pktErr, ok := err.(packet.Error); ok {
				connAck := newConnAck(&conn, pktErr.ReasonCode, false, cm.conf,
					nil)
				_, _ = cm.sendPacket(&conn, &connAck)
			}
			break
		}

		cm.metrics.recordPacketReceived(pkt)
		cm.log.Debug().
			Uint8("PacketTypeID", uint8(pkt.Type())).
			Msg("MQTT Received packet")

		err = cm.handlePacket(pkt, &conn)
		if err != nil {
			cm.log.Warn().
				Bytes("ClientID", conn.clientID).
				Stringer("PacketType", pkt.Type()).
				Msg("MQTT Failed to process packet: " + err.Error())
			break
		}

		if !conn.connected {
			break
		}

		deadline = nextConnectionDeadline(conn)
	}
}

// Close closes the given connection.
func (cm *ConnectionManager) Close(conn *Connection, force bool) {
	if conn.closed {
		return
	}

	if tcp, ok := conn.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0)
	}

	cm.log.Debug().
		Bytes("ClientID", conn.clientID).
		Bool("Force", force).
		Msg("MQTT Closing connection")

	_ = conn.netConn.Close()
	conn.connected = false
	conn.closed = true
	cm.metrics.recordDisconnection()
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
		cm.log.Trace().
			Bytes("ClientID", conn.clientID).
			Uint8("Version", uint8(conn.version)).
			Uint16("KeepAlive", conn.timeout).
			Msg("MQTT Handling PINGREQ Packet")

		return cm.sendPingResp(conn, pkt)
	case packet.DISCONNECT:
		cm.log.Trace().
			Bytes("ClientID", conn.clientID).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Handling DISCONNECT Packet")

		cm.Close(conn, false)
		cm.metrics.recordDisconnectLatency(time.Since(pkt.Timestamp()))
		return nil
	default:
		return errors.New("invalid packet type: " + pkt.Type().String())
	}
}

func (cm *ConnectionManager) handlePacketConnect(
	connPkt *packet.Connect,
	conn *Connection,
) error {
	cm.log.Trace().
		Bytes("ClientID", connPkt.ClientID).
		Uint8("Version", uint8(connPkt.Version)).
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
		code := packet.ReasonCodeV3IdentifierRejected
		connAck := newConnAck(conn, code, false, cm.conf, nil)

		errSendConnAck := cm.sendConnAck(conn, connPkt, &connAck)
		return multierr.Combine(err, errSendConnAck)
	}

	if err := cm.checkClientID(connPkt); err != nil {
		connAck := newConnAck(conn, err.ReasonCode, false, cm.conf, nil)

		errSendConnAck := cm.sendConnAck(conn, connPkt, &connAck)
		return multierr.Combine(err, errSendConnAck)
	}

	code := packet.ReasonCodeV3ConnectionAccepted
	connAck := newConnAck(conn, code, false, cm.conf, connPkt.Properties)

	if len(connPkt.ClientID) == 0 {
		conn.clientID = generateClientID(cm.conf.ClientIDPrefix)

		if conn.version == packet.MQTT50 {
			connAck.Properties = getPropertiesOrCreate(connAck.Properties)
			connAck.Properties.AssignedClientID = conn.clientID
		}
	}

	if conn.version == packet.MQTT50 {
		connAck.Properties = getPropertiesOrCreate(connAck.Properties)
		connAck.Properties.UserProperties = cm.userProperties
	}

	err := cm.sendConnAck(conn, connPkt, &connAck)
	if err != nil {
		return err
	}

	conn.connected = true
	if connAck.Properties != nil && connAck.Properties.ServerKeepAlive != nil {
		conn.timeout = *connAck.Properties.ServerKeepAlive
	}

	cm.log.Debug().
		Bytes("ClientID", connPkt.ClientID).
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

func (cm *ConnectionManager) sendConnAck(
	conn *Connection,
	connect *packet.Connect,
	connAck *packet.ConnAck,
) error {
	cm.log.Trace().
		Bytes("ClientID", conn.clientID).
		Uint8("PacketTypeID", uint8(connAck.Type())).
		Uint8("Version", uint8(conn.version)).
		Msg("MQTT Sending CONNACK Packet")

	tm, err := cm.sendPacket(conn, connAck)
	if err != nil {
		return err
	}

	latency := tm.Sub(connect.Timestamp())
	cm.metrics.recordConnectLatency(latency, int(connAck.ReasonCode))
	return nil
}

func (cm *ConnectionManager) sendPingResp(
	conn *Connection,
	pingReq packet.Packet,
) error {
	cm.log.Trace().
		Bytes("ClientID", conn.clientID).
		Uint8("PacketTypeID", uint8(pingReq.Type())).
		Uint16("Timeout", conn.timeout).
		Uint8("Version", uint8(conn.version)).
		Msg("MQTT Sending PINGRESP Packet")

	pingResp := packet.NewPingResp()

	tm, err := cm.sendPacket(conn, &pingResp)
	if err != nil {
		return err
	}

	latency := tm.Sub(pingReq.Timestamp())
	cm.metrics.recordPingLatency(latency)
	return nil
}

func (cm *ConnectionManager) sendPacket(
	conn *Connection,
	pkt packet.Packet,
) (time.Time, error) {
	cm.log.Debug().
		Bytes("ClientID", conn.clientID).
		Uint8("PacketTypeID", uint8(pkt.Type())).
		Uint8("Version", uint8(conn.version)).
		Msg("MQTT Sending packet")

	err := cm.writer.WritePacket(pkt, conn.netConn)
	if err != nil {
		cm.log.Error().
			Bytes("ClientID", conn.clientID).
			Stringer("PacketType", pkt.Type()).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		return time.Now(), err
	}

	cm.metrics.recordPacketSent(pkt)
	return time.Now(), nil
}
