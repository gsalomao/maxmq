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
	"math"
	"net"
	"sync"
	"time"

	"github.com/gsalomao/maxmq/pkg/logger"
	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"go.uber.org/multierr"
)

// ErrConnectionTimeout indicates that the broker didn't receive any packet
// within the expected time period.
var ErrConnectionTimeout = errors.New("timeout - no packet received")

// ErrProtocolError indicates that the broker received any invalid packet.
var ErrProtocolError = errors.New("protocol error")

type connection struct {
	netConn   net.Conn
	session   *Session
	version   packet.MQTTVersion
	clientID  ClientID
	timeout   int
	connected bool
}

func (c *connection) nextConnectionDeadline() time.Time {
	if c.timeout > 0 {
		timeout := math.Ceil(float64(c.timeout) * 1.5)
		return time.Now().Add(time.Duration(timeout) * time.Second)
	}

	// Zero value of time to disable the timeout
	return time.Time{}
}

type connectionManager struct {
	conf           *Configuration
	sessionManager *sessionManager
	log            *logger.Logger
	metrics        *metrics
	connections    map[SessionID]*connection
	mutex          sync.RWMutex
	reader         packet.Reader
	writer         packet.Writer
}

func newConnectionManager(
	conf *Configuration, idGen IDGenerator, log *logger.Logger,
) *connectionManager {
	conf.BufferSize = bufferSizeOrDefault(conf.BufferSize)
	conf.MaxPacketSize = maxPacketSizeOrDefault(conf.MaxPacketSize)
	conf.ConnectTimeout = connectTimeoutOrDefault(conf.ConnectTimeout)
	conf.MaximumQoS = maximumQosOrDefault(conf.MaximumQoS)
	conf.MaxTopicAlias = maxTopicAliasOrDefault(conf.MaxTopicAlias)
	conf.MaxInflightMessages = maxInflightMsgOrDefault(conf.MaxInflightMessages)
	conf.MaxClientIDLen = maxClientIDLenOrDefault(conf.MaxClientIDLen)

	userProps := make([]packet.UserProperty, 0, len(conf.UserProperties))
	for k, v := range conf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	rdOpts := packet.ReaderOptions{
		BufferSize:    conf.BufferSize,
		MaxPacketSize: conf.MaxPacketSize,
	}

	m := newMetrics(conf.MetricsEnabled, log)
	cm := connectionManager{
		conf:        conf,
		log:         log,
		metrics:     m,
		connections: make(map[SessionID]*connection),
		reader:      packet.NewReader(rdOpts),
		writer:      packet.NewWriter(conf.BufferSize),
	}

	cm.sessionManager = newSessionManager(&cm, idGen, conf, m, userProps, log)
	return &cm
}

func (m *connectionManager) start() {
	m.log.Trace().Msg("MQTT Starting connection manager")
	m.sessionManager.start()
}

func (m *connectionManager) stop() {
	m.log.Trace().Msg("MQTT Stopping connection manager")
	m.sessionManager.stop()
	m.log.Debug().Msg("MQTT Connection manager stopped with success")
}

func (m *connectionManager) handle(nc net.Conn) error {
	conn := m.createConnection(nc)
	defer m.closeConnection(&conn, true)

	m.metrics.recordConnection()
	m.log.Debug().
		Int("Timeout", conn.timeout).
		Msg("MQTT Handling connection")

	for {
		deadline := conn.nextConnectionDeadline()
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			m.log.Error().
				Msg("MQTT Failed to set read deadline: " + err.Error())
			return errors.New("failed to set read deadline: " + err.Error())
		}

		m.log.Trace().
			Float64("DeadlineIn", time.Until(deadline).Seconds()).
			Int("Timeout", conn.timeout).
			Msg("MQTT Waiting packet")

		pkt, err := m.readPacket(&conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = m.handlePacket(&conn, pkt)
		if err != nil {
			return err
		}

		if !conn.session.connected {
			m.closeConnection(&conn, false)
			return nil
		}
	}
}

func (m *connectionManager) readPacket(conn *connection) (pkt packet.Packet,
	err error) {

	pkt, err = m.reader.ReadPacket(conn.netConn, conn.version)
	if err != nil {
		if errors.Is(err, io.EOF) {
			m.log.Debug().
				Bytes("ClientId", conn.clientID).
				Msg("MQTT Network connection was closed")
			return nil, io.EOF
		}

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			m.log.Debug().
				Bytes("ClientId", conn.clientID).
				Bool("Connected", conn.connected).
				Int("Timeout", conn.timeout).
				Msg("MQTT Timeout - No packet received")
			return nil, ErrConnectionTimeout
		}

		m.log.Warn().
			Bytes("ClientId", conn.clientID).
			Bool("Connected", conn.connected).
			Int("Timeout", conn.timeout).
			Msg("MQTT Failed to read packet: " + err.Error())
		return nil, ErrProtocolError
	}

	m.metrics.recordPacketReceived(pkt)
	m.log.Debug().
		Bytes("ClientId", conn.clientID).
		Bool("Connected", conn.connected).
		Uint8("PacketTypeId", uint8(pkt.Type())).
		Msg("MQTT Received packet")
	return
}

func (m *connectionManager) handlePacket(conn *connection,
	pkt packet.Packet) error {

	var reply packet.Packet
	var err error

	conn.session, reply, err = m.sessionManager.handlePacket(conn.session, pkt)
	if err != nil {
		m.log.Warn().
			Bytes("ClientId", conn.clientID).
			Stringer("PacketType", pkt.Type()).
			Msg("MQTT Failed to handle packet: " + err.Error())
		err = errors.New("failed to handle packet: " + err.Error())
	}

	var newConnection bool
	if reply != nil {
		errReply := m.replyPacket(pkt, reply, conn)
		if errReply != nil {
			err = multierr.Combine(err,
				errors.New("failed to send packet: "+errReply.Error()))
			return err
		}

		if reply.Type() == packet.CONNACK {
			connAck := reply.(*packet.ConnAck)
			if connAck.ReasonCode == packet.ReasonCodeV3ConnectionAccepted {
				newConnection = true
			}
		}
	}

	if newConnection {
		conn.clientID = conn.session.ClientID
		conn.version = conn.session.Version
		conn.timeout = conn.session.KeepAlive

		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.connections[conn.session.SessionID] = conn
	}

	return err
}

func (m *connectionManager) createConnection(nc net.Conn) connection {
	return connection{
		netConn:   nc,
		timeout:   m.conf.ConnectTimeout,
		connected: true,
		version:   packet.MQTT311, // TODO: Add default version in the config
	}
}

func (m *connectionManager) closeConnection(conn *connection, force bool) {
	if !conn.connected {
		return
	}

	m.log.Trace().
		Bytes("ClientId", conn.clientID).
		Bool("Force", force).
		Msg("MQTT Closing connection")

	if tcp, ok := conn.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0)
	}

	_ = conn.netConn.Close()
	conn.connected = false

	if conn.session != nil {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		m.sessionManager.disconnectSession(conn.session)
		delete(m.connections, conn.session.SessionID)
	}

	m.metrics.recordDisconnection()
	m.log.Debug().
		Bytes("ClientId", conn.clientID).
		Bool("Force", force).
		Msg("MQTT Connection closed")
}

func (m *connectionManager) replyPacket(pkt packet.Packet,
	reply packet.Packet,
	c *connection) error {

	m.log.Trace().
		Bytes("ClientId", c.session.ClientID).
		Uint8("PacketTypeId", uint8(reply.Type())).
		Uint8("Version", uint8(c.session.Version)).
		Msg("MQTT Sending packet")

	err := m.writer.WritePacket(reply, c.netConn)
	if err != nil {
		m.log.Warn().
			Bytes("ClientId", c.session.ClientID).
			Stringer("PacketType", reply.Type()).
			Uint8("Version", uint8(c.session.Version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		err = multierr.Combine(err,
			errors.New("failed to send packet: "+err.Error()))
	} else {
		m.recordLatencyMetrics(pkt, reply)
		m.metrics.recordPacketSent(reply)
		m.log.Debug().
			Bytes("ClientId", c.session.ClientID).
			Uint8("PacketTypeId", uint8(reply.Type())).
			Uint8("Version", uint8(c.session.Version)).
			Msg("MQTT Packet sent with success")
	}

	return err
}

func (m *connectionManager) deliverPacket(id SessionID,
	pkt *packet.Publish) error {

	m.mutex.RLock()
	conn, ok := m.connections[id]
	m.mutex.RUnlock()
	if !ok {
		return errors.New("connection not found")
	}

	m.log.Trace().
		Bytes("ClientId", conn.clientID).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint8("Retain", pkt.Retain).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Delivering packet to client")

	err := m.writer.WritePacket(pkt, conn.netConn)
	if err != nil {
		return err
	}

	m.log.Debug().
		Bytes("ClientId", conn.clientID).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint8("Retain", pkt.Retain).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Packet delivered to client with success")
	return nil
}

func (m *connectionManager) recordLatencyMetrics(pkt packet.Packet,
	reply packet.Packet) {

	pktType := pkt.Type()
	replyType := reply.Type()
	latency := reply.Timestamp().Sub(pkt.Timestamp())

	if pktType == packet.CONNECT && replyType == packet.CONNACK {
		connAck := reply.(*packet.ConnAck)
		m.metrics.recordConnectLatency(latency, int(connAck.ReasonCode))
	} else if pktType == packet.PINGREQ && replyType == packet.PINGRESP {
		m.metrics.recordPingLatency(latency)
	} else if pktType == packet.SUBSCRIBE && replyType == packet.SUBACK {
		m.metrics.recordSubscribeLatency(latency)
	} else if pktType == packet.UNSUBSCRIBE && replyType == packet.UNSUBACK {
		m.metrics.recordUnsubscribeLatency(latency)
	}
}
