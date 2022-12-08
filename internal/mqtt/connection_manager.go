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
	"sync"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"go.uber.org/multierr"
)

var errConnectionTimeout = errors.New("timeout - no packet received")
var errProtocolError = errors.New("protocol error")

type connectionManager struct {
	conf           *Configuration
	sessionManager *sessionManager
	log            *logger.Logger
	metrics        *metrics
	connections    map[ClientID]*connection
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
		connections: make(map[ClientID]*connection),
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

		if !conn.connected {
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
				Str("ClientId", string(conn.clientID)).
				Msg("MQTT Network connection was closed")
			return nil, io.EOF
		}

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			m.log.Debug().
				Str("ClientId", string(conn.clientID)).
				Bool("Connected", conn.connected).
				Int("Timeout", conn.timeout).
				Msg("MQTT Timeout - No packet received")
			return nil, errConnectionTimeout
		}

		m.log.Warn().
			Str("ClientId", string(conn.clientID)).
			Bool("Connected", conn.connected).
			Int("Timeout", conn.timeout).
			Msg("MQTT Failed to read packet: " + err.Error())
		return nil, errProtocolError
	}

	m.metrics.recordPacketReceived(pkt)
	m.log.Debug().
		Str("ClientId", string(conn.clientID)).
		Bool("Connected", conn.connected).
		Uint8("PacketTypeId", uint8(pkt.Type())).
		Msg("MQTT Received packet")
	return
}

func (m *connectionManager) handlePacket(conn *connection,
	pkt packet.Packet) error {

	session, replies, err := m.sessionManager.handlePacket(conn.clientID, pkt)
	if err != nil {
		err = fmt.Errorf("failed to handle packet %v: %w",
			pkt.Type().String(), err)
	}

	var newConnection bool
	for _, reply := range replies {
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
		conn.clientID = session.ClientID
		conn.version = session.Version
		conn.timeout = session.KeepAlive
		conn.connected = session.connected
		conn.hasSession = true

		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.connections[conn.clientID] = conn
	}

	if session == nil || !session.connected {
		m.disconnect(conn)
		m.closeConnection(conn, false)
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

func (m *connectionManager) disconnect(conn *connection) {
	if conn.hasSession {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		conn.hasSession = false
		delete(m.connections, conn.clientID)
	}
}

func (m *connectionManager) closeConnection(conn *connection, force bool) {
	if !conn.connected && !conn.hasSession {
		return
	}

	m.log.Trace().
		Str("ClientId", string(conn.clientID)).
		Bool("Force", force).
		Msg("MQTT Closing connection")

	if tcp, ok := conn.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0)
	}

	_ = conn.netConn.Close()
	conn.connected = false

	if conn.hasSession {
		m.sessionManager.disconnectSession(conn.clientID)
		m.disconnect(conn)
	}

	m.metrics.recordDisconnection()
	m.log.Debug().
		Str("ClientId", string(conn.clientID)).
		Bool("Force", force).
		Msg("MQTT Connection closed")
}

func (m *connectionManager) replyPacket(pkt packet.Packet,
	reply packet.Packet, conn *connection) error {

	m.log.Trace().
		Str("ClientId", string(conn.clientID)).
		Uint8("PacketTypeId", uint8(reply.Type())).
		Uint8("Version", uint8(conn.version)).
		Msg("MQTT Sending packet")

	err := m.writer.WritePacket(reply, conn.netConn)
	if err != nil {
		m.log.Warn().
			Str("ClientId", string(conn.clientID)).
			Stringer("PacketType", reply.Type()).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		err = multierr.Combine(err,
			errors.New("failed to send packet: "+err.Error()))
	} else {
		m.recordLatencyMetrics(pkt, reply)
		m.metrics.recordPacketSent(reply)
		m.log.Debug().
			Str("ClientId", string(conn.clientID)).
			Uint8("PacketTypeId", uint8(reply.Type())).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Packet sent with success")
	}

	return err
}

func (m *connectionManager) deliverPacket(id ClientID,
	pkt *packet.Publish) error {

	m.mutex.RLock()
	conn, ok := m.connections[id]
	m.mutex.RUnlock()
	if !ok {
		return errors.New("connection not found")
	}

	m.log.Trace().
		Str("ClientId", string(conn.clientID)).
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
		Str("ClientId", string(conn.clientID)).
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