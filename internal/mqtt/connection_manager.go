// Copyright 2022-2023 The MaxMQ Authors
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
	conf *Configuration, mt *metrics, log *logger.Logger,
) *connectionManager {
	conf.BufferSize = bufferSizeOrDefault(conf.BufferSize)
	conf.ConnectTimeout = connectTimeoutOrDefault(conf.ConnectTimeout)
	conf.DefaultVersion = defaultVersionOrDefault(conf.DefaultVersion)
	conf.MaxPacketSize = maxPacketSizeOrDefault(conf.MaxPacketSize)
	conf.MaximumQoS = maximumQosOrDefault(conf.MaximumQoS)
	conf.MaxTopicAlias = maxTopicAliasOrDefault(conf.MaxTopicAlias)
	conf.MaxInflightMessages = maxInflightMsgOrDefault(conf.MaxInflightMessages)
	conf.MaxInflightRetries =
		maxInflightRetriesOrDefault(conf.MaxInflightRetries)
	conf.MaxClientIDLen = maxClientIDLenOrDefault(conf.MaxClientIDLen)

	rdOpts := packet.ReaderOptions{
		BufferSize:    conf.BufferSize,
		MaxPacketSize: conf.MaxPacketSize,
	}

	cm := connectionManager{
		conf:        conf,
		metrics:     mt,
		log:         log,
		connections: make(map[ClientID]*connection),
		reader:      packet.NewReader(rdOpts),
		writer:      packet.NewWriter(conf.BufferSize),
	}

	return &cm
}

func (cm *connectionManager) start() {
	cm.log.Trace().Msg("MQTT Starting connection manager")
	cm.sessionManager.start()
}

func (cm *connectionManager) stop() {
	cm.log.Trace().Msg("MQTT Stopping connection manager")
	cm.sessionManager.stop()
	cm.log.Debug().Msg("MQTT Connection manager stopped with success")
}

func (cm *connectionManager) handle(nc net.Conn) {
	conn := cm.createConnection(nc)
	defer cm.closeConnection(&conn, true)

	cm.metrics.recordConnection()
	cm.log.Debug().
		Int("Timeout", conn.timeout).
		Msg("MQTT Handling connection")

	for {
		deadline := conn.nextConnectionDeadline()
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(conn.clientID)).
				Bool("Connected", conn.connected).
				Float64("DeadlineIn", time.Until(deadline).Seconds()).
				Int("Timeout", conn.timeout).
				Int("Version", int(conn.version)).
				Msg("MQTT Failed to set read deadline: " + err.Error())
		}

		cm.log.Trace().
			Str("ClientId", string(conn.clientID)).
			Bool("Connected", conn.connected).
			Float64("DeadlineIn", time.Until(deadline).Seconds()).
			Int("Timeout", conn.timeout).
			Int("Version", int(conn.version)).
			Msg("MQTT Waiting packet")

		pkt, err := cm.readPacket(&conn)
		if err != nil {
			break
		}

		err = cm.handlePacket(&conn, pkt)
		if err != nil {
			break
		}

		if !conn.connected {
			break
		}
	}
}

func (cm *connectionManager) readPacket(conn *connection) (pkt packet.Packet,
	err error) {

	pkt, err = cm.reader.ReadPacket(conn.netConn, conn.version)
	if err != nil {
		if errors.Is(err, io.EOF) {
			cm.log.Debug().
				Str("ClientId", string(conn.clientID)).
				Bool("Connected", conn.connected).
				Int("Timeout", conn.timeout).
				Int("Version", int(conn.version)).
				Msg("MQTT Network connection was closed: " + err.Error())
			return nil, io.EOF
		}

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			cm.log.Debug().
				Str("ClientId", string(conn.clientID)).
				Bool("Connected", conn.connected).
				Int("Timeout", conn.timeout).
				Int("Version", int(conn.version)).
				Msg("MQTT Timeout - No packet received")
			return nil, errConnectionTimeout
		}

		cm.log.Error().
			Str("ClientId", string(conn.clientID)).
			Bool("Connected", conn.connected).
			Int("Timeout", conn.timeout).
			Int("Version", int(conn.version)).
			Msg("MQTT Failed to read packet: " + err.Error())
		return nil, errProtocolError
	}

	cm.metrics.recordPacketReceived(pkt)
	cm.log.Debug().
		Str("ClientId", string(conn.clientID)).
		Bool("Connected", conn.connected).
		Uint8("PacketTypeId", uint8(pkt.Type())).
		Int("Size", pkt.Size()).
		Int("Version", int(conn.version)).
		Msg("MQTT Received packet")
	return
}

func (cm *connectionManager) handlePacket(conn *connection,
	pkt packet.Packet) error {

	session, replies, err := cm.sessionManager.handlePacket(conn.clientID, pkt)
	if err != nil {
		cm.log.Error().
			Str("ClientId", string(conn.clientID)).
			Bool("Connected", conn.connected).
			Bool("HasSession", conn.hasSession).
			Int("Timeout", conn.timeout).
			Int("Version", int(conn.version)).
			Msg(fmt.Sprintf("MQTT Failed to handle packet %v: %v",
				pkt.Type().String(), err.Error()))
	}

	for _, reply := range replies {
		if reply.Type() == packet.CONNACK {
			connAck := reply.(*packet.ConnAck)
			if connAck.ReasonCode == packet.ReasonCodeV3ConnectionAccepted {
				conn.clientID = session.ClientID
				conn.version = session.Version
				conn.timeout = session.KeepAlive
				conn.connected = session.connected
				conn.hasSession = true
				cm.log.Debug().
					Str("ClientId", string(conn.clientID)).
					Bool("Connected", conn.connected).
					Bool("HasSession", conn.hasSession).
					Int("Timeout", conn.timeout).
					Int("Version", int(conn.version)).
					Msg("MQTT New Connection")

				cm.mutex.Lock()
				cm.connections[conn.clientID] = conn
				cm.mutex.Unlock()
			}
		}

		errReply := cm.replyPacket(pkt, reply, conn)
		if errReply != nil {
			cm.log.Warn().
				Str("ClientId", string(conn.clientID)).
				Bool("Connected", conn.connected).
				Bool("HasSession", conn.hasSession).
				Int("Timeout", conn.timeout).
				Int("Version", int(conn.version)).
				Msg("MQTT Failed to send reply: " + errReply.Error())

			err = multierr.Combine(err, errReply)
			return err
		}
	}
	if err != nil {
		return err
	}

	if !session.connected {
		cm.disconnect(conn)
		cm.closeConnection(conn, false)
	}

	return nil
}

func (cm *connectionManager) createConnection(nc net.Conn) connection {
	return connection{
		netConn:   nc,
		timeout:   cm.conf.ConnectTimeout,
		connected: true,
		version:   packet.MQTTVersion(cm.conf.DefaultVersion),
	}
}

func (cm *connectionManager) disconnect(conn *connection) {
	if conn.hasSession {
		cm.mutex.Lock()
		defer cm.mutex.Unlock()

		conn.hasSession = false
		delete(cm.connections, conn.clientID)
	}
}

func (cm *connectionManager) closeConnection(conn *connection, force bool) {
	if !conn.connected && !conn.hasSession {
		return
	}

	cm.log.Trace().
		Str("ClientId", string(conn.clientID)).
		Bool("Force", force).
		Msg("MQTT Closing connection")

	if tcp, ok := conn.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0)
	}

	_ = conn.netConn.Close()
	conn.connected = false

	if conn.hasSession {
		cm.sessionManager.disconnectSession(conn.clientID)
		cm.disconnect(conn)
	}

	cm.metrics.recordDisconnection()
	cm.log.Debug().
		Str("ClientId", string(conn.clientID)).
		Bool("Force", force).
		Msg("MQTT Connection closed")
}

func (cm *connectionManager) replyPacket(pkt packet.Packet,
	reply packet.Packet, conn *connection) error {

	cm.log.Trace().
		Str("ClientId", string(conn.clientID)).
		Uint8("PacketTypeId", uint8(reply.Type())).
		Int("Size", reply.Size()).
		Uint8("Version", uint8(conn.version)).
		Msg("MQTT Sending packet")

	err := cm.writer.WritePacket(reply, conn.netConn)
	if err != nil {
		cm.log.Warn().
			Str("ClientId", string(conn.clientID)).
			Stringer("PacketType", reply.Type()).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		err = multierr.Combine(err,
			errors.New("failed to send packet: "+err.Error()))
	} else {
		cm.recordLatencyMetrics(pkt, reply)
		cm.metrics.recordPacketSent(reply)
		cm.log.Debug().
			Str("ClientId", string(conn.clientID)).
			Uint8("PacketTypeId", uint8(reply.Type())).
			Int("Size", reply.Size()).
			Uint8("Version", uint8(conn.version)).
			Msg("MQTT Packet sent with success")
	}

	return err
}

func (cm *connectionManager) deliverPacket(id ClientID,
	pkt *packet.Publish) error {

	cm.mutex.RLock()
	conn, ok := cm.connections[id]
	cm.mutex.RUnlock()
	if !ok {
		return errors.New("connection not found")
	}

	cm.log.Trace().
		Str("ClientId", string(conn.clientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint8("Retain", pkt.Retain).
		Int("Size", pkt.Size()).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Delivering packet to client")

	err := cm.writer.WritePacket(pkt, conn.netConn)
	if err != nil {
		return err
	}

	cm.log.Debug().
		Str("ClientId", string(conn.clientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint8("Retain", pkt.Retain).
		Int("Size", pkt.Size()).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Packet delivered to client with success")
	return nil
}

func (cm *connectionManager) recordLatencyMetrics(pkt packet.Packet,
	reply packet.Packet) {

	pktType := pkt.Type()
	replyType := reply.Type()
	latency := reply.Timestamp().Sub(pkt.Timestamp())

	if pktType == packet.CONNECT && replyType == packet.CONNACK {
		connAck := reply.(*packet.ConnAck)
		cm.metrics.recordConnectLatency(latency, int(connAck.ReasonCode))
	} else if pktType == packet.PINGREQ && replyType == packet.PINGRESP {
		cm.metrics.recordPingLatency(latency)
	} else if pktType == packet.SUBSCRIBE && replyType == packet.SUBACK {
		cm.metrics.recordSubscribeLatency(latency)
	} else if pktType == packet.UNSUBSCRIBE && replyType == packet.UNSUBACK {
		cm.metrics.recordUnsubscribeLatency(latency)
	}
}
