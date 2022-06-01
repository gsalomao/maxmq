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
	"fmt"
	"io"
	"net"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"go.uber.org/multierr"
)

// ClientID represents the MQTT Client ID.
type ClientID []byte

// Connection represents a network connection.
type Connection struct {
	session   Session
	netConn   net.Conn
	timeout   uint16
	version   packet.MQTTVersion
	connected bool
	closed    bool
}

// ConnectionHandler is responsible for handle connections.
type ConnectionHandler interface {
	// NewConnection creates a new Connection.
	NewConnection(nc net.Conn) Connection

	// Handle handles the Connection.
	Handle(conn Connection) error
}

// ConnectionManager implements the ConnectionHandler interface.
type ConnectionManager struct {
	conf           Configuration
	log            *logger.Logger
	metrics        *metrics
	userProperties []packet.UserProperty
	reader         packet.Reader
	writer         packet.Writer
	sessionStore   SessionStore
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(
	cf Configuration,
	st SessionStore,
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

	rdOpts := packet.ReaderOptions{
		BufferSize:    cf.BufferSize,
		MaxPacketSize: cf.MaxPacketSize,
	}

	lg.Trace().Msg("MQTT Creating Connection Manager")
	return ConnectionManager{
		conf:           cf,
		log:            lg,
		metrics:        newMetrics(cf.MetricsEnabled, lg),
		userProperties: userProps,
		reader:         packet.NewReader(rdOpts),
		writer:         packet.NewWriter(cf.BufferSize),
		sessionStore:   st,
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
func (cm *ConnectionManager) Handle(conn Connection) error {
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
			return errors.New("failed to set read deadline: " + err.Error())
		}

		cm.log.Trace().
			Uint16("Timeout", conn.timeout).
			Msg("MQTT Waiting packet")

		pkt, err := cm.reader.ReadPacket(conn.netConn, conn.version)
		if err != nil {
			if err == io.EOF {
				cm.log.Debug().Msg("MQTT Connection was closed")
				return nil
			}

			if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
				cm.log.Debug().
					Bytes("ClientID", conn.session.ClientID).
					Bool("Connected", conn.connected).
					Uint16("Timeout", conn.timeout).
					Msg("MQTT Timeout - No packet received")
				return errors.New("timeout - no packet received")
			}

			cm.log.Info().Msg("MQTT Failed to read packet: " + err.Error())

			if pktErr, ok := err.(packet.Error); ok {
				connAck := newConnAck(&conn, pktErr.ReasonCode, false, cm.conf,
					nil)
				_, _ = cm.sendPacket(&conn, &connAck)
			}
			return errors.New("failed to read packet: " + err.Error())
		}

		cm.metrics.recordPacketReceived(pkt)
		cm.log.Debug().
			Uint8("PacketTypeID", uint8(pkt.Type())).
			Msg("MQTT Received packet")

		err = cm.handlePacket(pkt, &conn)
		if err != nil {
			cm.log.Warn().
				Bytes("ClientID", conn.session.ClientID).
				Stringer("PacketType", pkt.Type()).
				Msg("MQTT Failed to process packet: " + err.Error())
			return errors.New("failed to process packet: " + err.Error())
		}

		if !conn.connected {
			return nil
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
		Bytes("ClientID", conn.session.ClientID).
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
		return fmt.Errorf("received %v before CONNECT", pkt.Type())
	}

	switch pkt.Type() {
	case packet.CONNECT:
		connPkt, _ := pkt.(*packet.Connect)
		return cm.handlePacketConnect(connPkt, conn)
	case packet.PINGREQ:
		cm.log.Trace().
			Bytes("ClientID", conn.session.ClientID).
			Uint8("Version", uint8(conn.version)).
			Uint16("KeepAlive", conn.timeout).
			Msg("MQTT Handling PINGREQ Packet")

		return cm.sendPingResp(conn, pkt)
	case packet.DISCONNECT:
		cm.log.Trace().
			Bytes("ClientID", conn.session.ClientID).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Handling DISCONNECT Packet")

		cm.Close(conn, false)
		err := cm.sessionStore.DeleteSession(conn.session)
		if err == nil {
			cm.log.Debug().
				Bytes("ClientID", conn.session.ClientID).
				Msg("MQTT Session deleted with success")
		} else {
			cm.log.Error().
				Bytes("ClientID", conn.session.ClientID).
				Msg("MQTT Failed to delete session: " + err.Error())
		}

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

	clientID := connPkt.ClientID
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

	// Get the session expiry interval before create ConnAck, as the Properties
	// from Connect Packet is reset and reused to create the ConnAck Packet.
	sessionExp := getSessionExpiryInterval(connPkt,
		cm.conf.MaxSessionExpiryInterval)

	var err error
	generatedID := false

	if len(connPkt.ClientID) == 0 {
		clientID = generateClientID(cm.conf.ClientIDPrefix)
		generatedID = true
	}

	conn.session, err = cm.findOrCreateSession(clientID, sessionExp)
	if err != nil {
		return err
	}

	code := packet.ReasonCodeV3ConnectionAccepted
	connAck := newConnAck(conn, code, false, cm.conf, connPkt.Properties)
	connPkt.Properties = nil

	if conn.version == packet.MQTT50 {
		connAck.Properties = getPropertiesOrCreate(connAck.Properties)
		connAck.Properties.UserProperties = cm.userProperties

		if generatedID {
			connAck.Properties.AssignedClientID = clientID
		}
	}

	err = cm.sendConnAck(conn, connPkt, &connAck)
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

	if cIDLen == 0 && !cm.conf.AllowEmptyClientID {
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
		Bytes("ClientID", conn.session.ClientID).
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
		Bytes("ClientID", conn.session.ClientID).
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
		Bytes("ClientID", conn.session.ClientID).
		Uint8("PacketTypeID", uint8(pkt.Type())).
		Uint8("Version", uint8(conn.version)).
		Msg("MQTT Sending packet")

	err := cm.writer.WritePacket(pkt, conn.netConn)
	if err != nil {
		cm.log.Error().
			Bytes("ClientID", conn.session.ClientID).
			Stringer("PacketType", pkt.Type()).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		return time.Now(), err
	}

	cm.metrics.recordPacketSent(pkt)
	return time.Now(), nil
}

func (cm *ConnectionManager) findOrCreateSession(id ClientID,
	expiration uint32) (Session, error) {

	var session Session
	var err error

	session, err = cm.sessionStore.GetSession(id)
	if err != nil {
		if err != ErrSessionNotFound {
			return session, err
		}

		cm.log.Debug().
			Bytes("ClientID", id).
			Msg("MQTT Session not found")

		session = Session{
			ClientID:       id,
			ExpiryInterval: expiration,
		}
	} else {
		cm.log.Debug().
			Bytes("ClientID", id).
			Int64("ConnectedAt", session.ConnectedAt).
			Msg("MQTT Session found")
	}

	session.ConnectedAt = time.Now().Unix()
	err = cm.sessionStore.SaveSession(session)
	if err != nil {
		cm.log.Error().
			Bytes("ClientID", id).
			Msg("MQTT Failed to save session: " + err.Error())
		return session, err
	}

	cm.log.Debug().
		Bytes("ClientID", id).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Msg("MQTT Session saved with success")

	return session, nil
}
