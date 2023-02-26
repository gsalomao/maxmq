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
	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"go.uber.org/multierr"
)

var errConnectionTimeout = errors.New("timeout - no packet received")
var errProtocolError = errors.New("protocol error")

type packetHandler interface {
	// HandlePacket handles the received packet from client.
	HandlePacket(id packet.ClientID, pkt packet.Packet) ([]packet.Packet, error)
}

type connectionManager struct {
	conf         *handler.Configuration
	sessionStore handler.SessionStore
	log          *logger.Logger
	metrics      *metrics
	pubSub       *pubSubManager
	connections  map[packet.ClientID]*connection
	handlers     map[packet.Type]packetHandler
	mutex        sync.RWMutex
	reader       packet.Reader
	writer       packet.Writer
}

func newConnectionManager(c *handler.Configuration, st handler.SessionStore,
	mt *metrics, idGen IDGenerator, l *logger.Logger) *connectionManager {

	c.BufferSize = bufferSizeOrDefault(c.BufferSize)
	c.ConnectTimeout = connectTimeoutOrDefault(c.ConnectTimeout)
	c.DefaultVersion = defaultVersionOrDefault(c.DefaultVersion)
	c.MaxPacketSize = maxPacketSizeOrDefault(c.MaxPacketSize)
	c.MaximumQoS = maximumQosOrDefault(c.MaximumQoS)
	c.MaxTopicAlias = maxTopicAliasOrDefault(c.MaxTopicAlias)
	c.MaxInflightMessages = maxInflightMsgOrDefault(c.MaxInflightMessages)
	c.MaxInflightRetries = maxInflightRetriesOrDefault(c.MaxInflightRetries)
	c.MaxClientIDLen = maxClientIDLenOrDefault(c.MaxClientIDLen)

	rdOpts := packet.ReaderOptions{
		BufferSize:    c.BufferSize,
		MaxPacketSize: c.MaxPacketSize,
	}

	cm := &connectionManager{
		conf:         c,
		sessionStore: st,
		metrics:      mt,
		log:          l,
		connections:  make(map[packet.ClientID]*connection),
		reader:       packet.NewReader(rdOpts),
		writer:       packet.NewWriter(c.BufferSize),
	}

	ps := newPubSubManager(cm, st, mt, l)
	cm.pubSub = ps
	cm.handlers = map[packet.Type]packetHandler{
		packet.CONNECT:     handler.NewConnectHandler(c, st, l),
		packet.DISCONNECT:  handler.NewDisconnectHandler(st, ps, l),
		packet.PINGREQ:     handler.NewPingReqHandler(st, l),
		packet.SUBSCRIBE:   handler.NewSubscribeHandler(c, st, ps, l),
		packet.UNSUBSCRIBE: handler.NewUnsubscribeHandler(st, ps, l),
		packet.PUBLISH:     handler.NewPublishHandler(st, ps, idGen, l),
		packet.PUBACK:      handler.NewPubAckHandler(st, l),
		packet.PUBREC:      handler.NewPubRecHandler(st, l),
		packet.PUBREL:      handler.NewPubRelHandler(st, ps, l),
		packet.PUBCOMP:     handler.NewPubCompHandler(st, l),
	}

	return cm
}

func (cm *connectionManager) start() {
	cm.log.Trace().Msg("MQTT Starting connection manager")
	cm.pubSub.start()
}

func (cm *connectionManager) stop() {
	cm.log.Trace().Msg("MQTT Stopping connection manager")
	cm.pubSub.stop()
	cm.log.Debug().Msg("MQTT Connection manager stopped with success")
}

func (cm *connectionManager) handle(conn connection) {
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
			break
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

	var replies []packet.Packet
	var err error

	hd, ok := cm.handlers[pkt.Type()]
	if ok {
		replies, err = hd.HandlePacket(conn.clientID, pkt)
	} else {
		err = errors.New("invalid packet type")
	}
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
			if connAck.ReasonCode == packet.ReasonCodeSuccess {
				conn.clientID = connAck.ClientID
				conn.version = connAck.Version
				conn.timeout = connAck.KeepAlive
				conn.connected = true
				conn.hasSession = true
				cm.log.Debug().
					Str("ClientId", string(conn.clientID)).
					Bool("Connected", conn.connected).
					Bool("HasSession", conn.hasSession).
					Int("Timeout", conn.timeout).
					Int("Version", int(conn.version)).
					Msg("MQTT New connection")

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

		if reply.Type() == packet.DISCONNECT {
			conn.hasSession = false
			cm.closeConnection(conn, false)
			break
		}
	}
	if err != nil {
		return err
	}

	if pkt.Type() == packet.DISCONNECT {
		conn.hasSession = false
		cm.closeConnection(conn, false)

		latency := time.Since(pkt.Timestamp())
		cm.metrics.recordDisconnectLatency(latency)
	}

	return nil
}

func (cm *connectionManager) newConnection(nc net.Conn) connection {
	return connection{
		netConn: nc,
		timeout: cm.conf.ConnectTimeout,
		version: packet.Version(cm.conf.DefaultVersion),
	}
}

func (cm *connectionManager) disconnectSession(conn *connection) {
	s, err := cm.sessionStore.ReadSession(conn.clientID)
	if err != nil {
		cm.log.Error().
			Str("ClientId", string(conn.clientID)).
			Bool("Connected", conn.connected).
			Bool("HasSession", conn.hasSession).
			Int("Timeout", conn.timeout).
			Int("Version", int(conn.version)).
			Msg("MQTT Failed to read session (CONNMGR): " + err.Error())
		return
	}

	if s.CleanSession {
		err = cm.sessionStore.DeleteSession(s)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to delete session (CONNMGR): " + err.Error())
			return
		}
	} else {
		s.Connected = false
		err = cm.sessionStore.SaveSession(s)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to save session (CONNMGR): " + err.Error())
			return
		}
	}
	conn.hasSession = false
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

	if conn.connected {
		cm.mutex.Lock()
		delete(cm.connections, conn.clientID)
		conn.connected = false
		cm.mutex.Unlock()
	}

	if conn.hasSession {
		cm.disconnectSession(conn)
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

	err := cm.writer.WritePacket(conn.netConn, reply)
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

func (cm *connectionManager) deliverPacket(id packet.ClientID,
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

	err := cm.writer.WritePacket(conn.netConn, pkt)
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
