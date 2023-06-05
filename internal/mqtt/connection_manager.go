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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/gsalomao/maxmq/internal/safe"
	"go.uber.org/multierr"
)

var errConnectionTimeout = errors.New("timeout - no packet received")
var errProtocolError = errors.New("protocol error")

// IDGenerator generates an identifier numbers.
type IDGenerator interface {
	// NextID generates the next identifier number.
	NextID() uint64
}

type packetHandler interface {
	handlePacket(id packet.ClientID, pkt packet.Packet) (replies []packet.Packet, err error)
}

type connectionManager struct {
	log                *logger.Logger
	metrics            *metrics
	sessionStore       *sessionStore
	pubSub             *pubSub
	conf               Config
	connections        safe.Value[map[packet.ClientID]*connection]
	pendingConnections safe.Value[[]*connection] // TODO: Replace with a linked list
	handlers           map[packet.Type]packetHandler
	reader             packet.Reader
	writer             packet.Writer
}

func newConnectionManager(c Config, ss *sessionStore, mt *metrics, g IDGenerator, l *logger.Logger) *connectionManager {
	c.BufferSize = bufferSizeOrDefault(c.BufferSize)
	c.ConnectTimeout = connectTimeoutOrDefault(c.ConnectTimeout)
	c.DefaultVersion = defaultVersionOrDefault(c.DefaultVersion)
	c.MaxPacketSize = maxPacketSizeOrDefault(c.MaxPacketSize)
	c.MaximumQoS = maximumQosOrDefault(c.MaximumQoS)
	c.MaxTopicAlias = maxTopicAliasOrDefault(c.MaxTopicAlias)
	c.MaxInflightMessages = maxInflightMsgOrDefault(c.MaxInflightMessages)
	c.MaxInflightRetries = maxInflightRetriesOrDefault(c.MaxInflightRetries)
	c.MaxClientIDLen = maxClientIDLenOrDefault(c.MaxClientIDLen)

	rdOpts := packet.ReaderOptions{BufferSize: c.BufferSize, MaxPacketSize: c.MaxPacketSize}

	cm := &connectionManager{
		log:                l.WithPrefix("mqtt.connmgr"),
		metrics:            mt,
		sessionStore:       ss,
		conf:               c,
		pendingConnections: safe.NewValue(make([]*connection, 0)),
		connections:        safe.NewValue(make(map[packet.ClientID]*connection)),
		reader:             packet.NewReader(rdOpts),
		writer:             packet.NewWriter(c.BufferSize),
	}

	ps := newPubSub(cm, ss, mt, l)
	cm.pubSub = ps
	cm.handlers = map[packet.Type]packetHandler{
		packet.CONNECT:     newConnectHandler(c, ss, l),
		packet.DISCONNECT:  newDisconnectHandler(ss, ps, l),
		packet.PINGREQ:     newPingReqHandler(ss, l),
		packet.SUBSCRIBE:   newSubscribeHandler(c, ss, ps, l),
		packet.UNSUBSCRIBE: newUnsubscribeHandler(ss, ps, l),
		packet.PUBLISH:     newPublishHandler(ss, ps, g, l),
		packet.PUBACK:      newPubAckHandler(ss, l),
		packet.PUBREC:      newPubRecHandler(ss, l),
		packet.PUBREL:      newPubRelHandler(ss, ps, l),
		packet.PUBCOMP:     newPubCompHandler(ss, l),
	}

	return cm
}

func (cm *connectionManager) newConnection(nc net.Conn) *connection {
	return &connection{
		netConn: nc,
		timeout: cm.conf.ConnectTimeout,
		version: packet.Version(cm.conf.DefaultVersion),
	}
}

func (cm *connectionManager) addPendingConnection(c *connection) {
	cm.pendingConnections.Lock()
	defer cm.pendingConnections.Unlock()

	cm.pendingConnections.Value = append(cm.pendingConnections.Value, c)
}

func (cm *connectionManager) addActiveConnection(c *connection) {
	cm.connections.Lock()
	defer cm.connections.Unlock()

	cm.connections.Value[c.clientID] = c
}

func (cm *connectionManager) serveConnection(c *connection) {
	defer cm.closeConnection(c, true)

	cm.metrics.recordConnection()
	cm.log.Debug().Int("Timeout", c.timeout).Msg("Serving connection")

	for {
		deadline := c.nextDeadline()
		err := c.netConn.SetReadDeadline(deadline)

		if err != nil {
			cm.log.Error().
				Str("ClientId", string(c.clientID)).
				Bool("Connected", c.connected()).
				Float64("Deadline", time.Until(deadline).Seconds()).
				Int("Timeout", c.timeout).
				Int("Version", int(c.version)).
				Msg("Failed to set read deadline: " + err.Error())
			break
		}

		cm.log.Trace().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected()).
			Float64("Deadline", time.Until(deadline).Seconds()).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg("Waiting packet")

		var pkt packet.Packet
		pkt, err = cm.readPacket(c)
		if err != nil {
			break
		}

		err = cm.handlePacket(c, pkt)
		if err != nil {
			break
		}

		if !c.connected() {
			break
		}
	}
}

func (cm *connectionManager) readPacket(c *connection) (pkt packet.Packet, err error) {
	pkt, err = cm.reader.ReadPacket(c.netConn, c.version)
	if err != nil {
		if errors.Is(err, io.EOF) || !c.connected() {
			cm.log.Debug().
				Str("ClientId", string(c.clientID)).
				Bool("Connected", c.connected()).
				Int("Timeout", c.timeout).
				Int("Version", int(c.version)).
				Msg("Network connection was closed")
			return nil, io.EOF
		}

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			cm.log.Debug().
				Str("ClientId", string(c.clientID)).
				Bool("Connected", c.connected()).
				Int("Timeout", c.timeout).
				Int("Version", int(c.version)).
				Msg("Timeout - No packet received")
			return nil, errConnectionTimeout
		}

		cm.log.Error().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected()).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg("Failed to read packet: " + err.Error())
		return nil, errProtocolError
	}

	cm.metrics.recordPacketReceived(pkt)
	cm.log.Debug().
		Str("ClientId", string(c.clientID)).
		Bool("Connected", c.connected()).
		Uint8("PacketTypeId", uint8(pkt.Type())).
		Int("Size", pkt.Size()).
		Int("Version", int(c.version)).
		Msg("Received packet")
	return pkt, nil
}

func (cm *connectionManager) handlePacket(c *connection, p packet.Packet) error {
	var replies []packet.Packet
	var err error

	c.setState(stateActive)

	h, ok := cm.handlers[p.Type()]
	if ok {
		replies, err = h.handlePacket(c.clientID, p)
	} else {
		err = errors.New("invalid packet type")
	}
	if err != nil {
		cm.log.Error().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected()).
			Bool("HasSession", c.hasSession).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg(fmt.Sprintf("Failed to handle packet %v: %v", p.Type().String(), err.Error()))
	}

	var code packet.ReasonCode
	for _, reply := range replies {
		if reply.Type() == packet.CONNACK {
			ack := reply.(*packet.ConnAck)
			code = ack.ReasonCode
			if ack.ReasonCode == packet.ReasonCodeSuccess {
				c.clientID = ack.ClientID
				c.version = ack.Version
				c.timeout = ack.KeepAlive
				c.hasSession = true
				cm.log.Debug().
					Str("ClientId", string(c.clientID)).
					Bool("Connected", c.connected()).
					Bool("HasSession", c.hasSession).
					Int("Timeout", c.timeout).
					Int("Version", int(c.version)).
					Msg("New MQTT connection")

				cm.removePendingConnection(c)
				cm.addActiveConnection(c)
			}
		}

		if rplErr := cm.replyPacket(reply, c); rplErr != nil {
			err = multierr.Combine(err, rplErr)
			break
		}

		cm.metrics.recordPacketSent(reply)
		cm.log.Debug().
			Str("ClientId", string(c.clientID)).
			Uint8("PacketTypeId", uint8(reply.Type())).
			Int("Size", reply.Size()).
			Uint8("Version", uint8(c.version)).
			Msg("Packet sent with success")

		if reply.Type() == packet.DISCONNECT {
			c.hasSession = false
			cm.closeConnection(c, false)
			break
		}
	}

	if err == nil {
		if p.Type() == packet.DISCONNECT {
			c.hasSession = false
			cm.closeConnection(c, false)

			latency := time.Since(p.Timestamp())
			cm.metrics.recordDisconnectLatency(latency)
		}

		latency := time.Since(p.Timestamp())
		cm.recordLatencyMetrics(p.Type(), code, latency)
		cm.log.Info().
			Str("ClientId", string(c.clientID)).
			Dur("LatencyInMs", latency).
			Uint8("Version", uint8(c.version)).
			Msg(fmt.Sprintf("Packet %v processed with success", p.Type().String()))
	}

	if c.connected() {
		c.setState(stateIdle)
	}

	return err
}

func (cm *connectionManager) replyPacket(reply packet.Packet, c *connection) error {
	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Uint8("PacketTypeId", uint8(reply.Type())).
		Int("Size", reply.Size()).
		Uint8("Version", uint8(c.version)).
		Msg("Sending packet")

	err := cm.writer.WritePacket(c.netConn, reply)
	if err != nil {
		cm.log.Warn().
			Str("ClientId", string(c.clientID)).
			Stringer("PacketType", reply.Type()).
			Uint8("Version", uint8(c.version)).
			Msg("Failed to send packet: " + err.Error())
		return fmt.Errorf("failed to send packet: %w", err)
	}
	return nil
}

func (cm *connectionManager) closeConnection(c *connection, force bool) {
	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Bool("Force", force).
		Msg("Closing connection")

	cm.pendingConnections.Lock()
	defer cm.pendingConnections.Unlock()

	cm.connections.Lock()
	defer cm.connections.Unlock()

	cm.closeConnectionLocked(c, force)
}

func (cm *connectionManager) closeConnectionLocked(c *connection, force bool) {
	if c.state() == stateClosed {
		cm.log.Trace().
			Str("ClientId", string(c.clientID)).
			Bool("Force", force).
			Msg("Connection already closed (Locked)")
		return
	}

	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Bool("Force", force).
		Msg("Closing connection (Locked)")

	state := c.state()
	c.close(force)

	if state == stateNew {
		cm.removePendingConnectionLocked(c)
	} else {
		delete(cm.connections.Value, c.clientID)
	}

	if c.hasSession {
		cm.disconnectSession(c)
	}

	cm.metrics.recordDisconnection()
	cm.log.Debug().
		Str("ClientId", string(c.clientID)).
		Bool("Force", force).
		Msg("Connection closed")
}

func (cm *connectionManager) removePendingConnection(c *connection) {
	cm.pendingConnections.Lock()
	defer cm.pendingConnections.Unlock()

	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Bool("Connected", c.connected()).
		Bool("HasSession", c.hasSession).
		Int("Timeout", c.timeout).
		Int("Version", int(c.version)).
		Msg("Removing pending connection (Locked)")
	cm.removePendingConnectionLocked(c)
}

func (cm *connectionManager) removePendingConnectionLocked(c *connection) {
	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Bool("Connected", c.connected()).
		Bool("HasSession", c.hasSession).
		Int("Timeout", c.timeout).
		Int("Version", int(c.version)).
		Msg("Removing pending connection (Locked)")

	var l []*connection
	if len(cm.pendingConnections.Value) > 1 {
		l = make([]*connection, 0, len(cm.pendingConnections.Value)-1)
		for _, cn := range cm.pendingConnections.Value {
			if cn.clientID != c.clientID {
				l = append(l, cn)
			}
		}
	} else {
		l = make([]*connection, 0)
	}

	cm.pendingConnections.Value = l
}

func (cm *connectionManager) closeIdleConnections() bool {
	cm.pendingConnections.Lock()
	defer cm.pendingConnections.Unlock()

	cm.connections.Lock()
	defer cm.connections.Unlock()

	for _, cn := range cm.pendingConnections.Value {
		cm.closeConnectionLocked(cn, false)
	}

	quiescent := true
	for _, cn := range cm.connections.Value {
		if cn.state() == stateIdle {
			cm.closeConnectionLocked(cn, false)
		} else {
			quiescent = false
		}
	}

	return quiescent
}

func (cm *connectionManager) closeAllConnections() {
	cm.pendingConnections.Lock()
	defer cm.pendingConnections.Unlock()

	cm.connections.Lock()
	defer cm.connections.Unlock()

	for _, cn := range cm.pendingConnections.Value {
		cm.closeConnectionLocked(cn, true)
	}

	for _, cn := range cm.connections.Value {
		cm.closeConnectionLocked(cn, true)
	}
}

func (cm *connectionManager) disconnectSession(c *connection) {
	s, err := cm.sessionStore.readSession(c.clientID)
	if err != nil {
		cm.log.Error().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected()).
			Bool("HasSession", c.hasSession).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg("Failed to read session (CONNMGR): " + err.Error())
		return
	}

	if s.cleanSession {
		for _, sub := range s.subscriptions {
			err = cm.pubSub.unsubscribe(s.clientID, sub.topicFilter)
			if err != nil {
				cm.log.Error().
					Str("ClientId", string(s.clientID)).
					Bool("Connected", s.connected).
					Uint64("SessionId", uint64(s.sessionID)).
					Str("Topic", sub.topicFilter).
					Uint8("Version", uint8(s.version)).
					Msg("Failed to unsubscribe topic")
			}
			delete(s.subscriptions, sub.topicFilter)
		}

		err = cm.sessionStore.deleteSession(s)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to delete session (CONNMGR): " + err.Error())
			return
		}
	} else {
		s.connected = false
		err = cm.sessionStore.saveSession(s)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to save session (CONNMGR): " + err.Error())
			return
		}
	}
	c.hasSession = false
}

func (cm *connectionManager) wait(ctx context.Context) {
	cm.pubSub.wait(ctx)
}

func (cm *connectionManager) recordLatencyMetrics(t packet.Type, rc packet.ReasonCode, latency time.Duration) {
	if t == packet.CONNECT {
		cm.metrics.recordConnectLatency(latency, int(rc))
	} else if t == packet.PINGREQ {
		cm.metrics.recordPingLatency(latency)
	} else if t == packet.SUBSCRIBE {
		cm.metrics.recordSubscribeLatency(latency)
	} else if t == packet.UNSUBSCRIBE {
		cm.metrics.recordUnsubscribeLatency(latency)
	}
}

func (cm *connectionManager) deliverPacket(id packet.ClientID, p *packet.Publish) error {
	cm.connections.RLock()
	c, ok := cm.connections.Value[id]
	cm.connections.RUnlock()

	if !ok {
		return errors.New("connection not found")
	}

	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Uint16("PacketId", uint16(p.PacketID)).
		Uint8("QoS", uint8(p.QoS)).
		Uint8("Retain", p.Retain).
		Int("Size", p.Size()).
		Str("TopicName", p.TopicName).
		Uint8("Version", uint8(p.Version)).
		Msg("Delivering packet to client")

	err := cm.writer.WritePacket(c.netConn, p)
	if err != nil {
		return err
	}

	cm.log.Debug().
		Str("ClientId", string(c.clientID)).
		Uint16("PacketId", uint16(p.PacketID)).
		Uint8("QoS", uint8(p.QoS)).
		Uint8("Retain", p.Retain).
		Int("Size", p.Size()).
		Str("TopicName", p.TopicName).
		Uint8("Version", uint8(p.Version)).
		Msg("Packet delivered to client with success")
	return nil
}
