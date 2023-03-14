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
	HandlePacket(id packet.ClientID, p packet.Packet) ([]packet.Packet, error)
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

func newConnectionManager(
	c *handler.Configuration,
	st handler.SessionStore,
	mt *metrics,
	idGen IDGenerator,
	l *logger.Logger,
) *connectionManager {
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
		conf:         c,
		sessionStore: st,
		metrics:      mt,
		log:          l.WithPrefix("connmgr"),
		connections:  make(map[packet.ClientID]*connection),
		reader:       packet.NewReader(rdOpts),
		writer:       packet.NewWriter(c.BufferSize),
	}

	ps := newPubSubManager(cm, st, mt, l)
	cm.pubSub = ps
	hl := l.WithPrefix("handler")
	cm.handlers = map[packet.Type]packetHandler{
		packet.CONNECT:     handler.NewConnectHandler(c, st, hl),
		packet.DISCONNECT:  handler.NewDisconnectHandler(st, ps, hl),
		packet.PINGREQ:     handler.NewPingReqHandler(st, hl),
		packet.SUBSCRIBE:   handler.NewSubscribeHandler(c, st, ps, hl),
		packet.UNSUBSCRIBE: handler.NewUnsubscribeHandler(st, ps, hl),
		packet.PUBLISH:     handler.NewPublishHandler(st, ps, idGen, hl),
		packet.PUBACK:      handler.NewPubAckHandler(st, hl),
		packet.PUBREC:      handler.NewPubRecHandler(st, hl),
		packet.PUBREL:      handler.NewPubRelHandler(st, ps, hl),
		packet.PUBCOMP:     handler.NewPubCompHandler(st, hl),
	}

	return cm
}

func (cm *connectionManager) start() {
	cm.log.Trace().Msg("Starting connection manager")
	cm.pubSub.start()
}

func (cm *connectionManager) stop() {
	cm.log.Trace().Msg("Stopping connection manager")
	cm.pubSub.stop()
	cm.log.Debug().Msg("Connection manager stopped with success")
}

func (cm *connectionManager) handle(c connection) {
	defer cm.closeConnection(&c, true /*force*/)

	cm.metrics.recordConnection()
	cm.log.Debug().Int("Timeout", c.timeout).Msg("Handling connection")

	for {
		deadline := c.nextConnectionDeadline()
		err := c.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(c.clientID)).
				Bool("Connected", c.connected).
				Float64("DeadlineIn", time.Until(deadline).Seconds()).
				Int("Timeout", c.timeout).
				Int("Version", int(c.version)).
				Msg("Failed to set read deadline: " + err.Error())
			break
		}

		cm.log.Trace().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected).
			Float64("DeadlineIn", time.Until(deadline).Seconds()).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg("Waiting packet")

		pkt, err := cm.readPacket(&c)
		if err != nil {
			break
		}

		err = cm.handlePacket(&c, pkt)
		if err != nil {
			break
		}

		if !c.connected {
			break
		}
	}
}

func (cm *connectionManager) readPacket(c *connection) (packet.Packet, error) {
	p, err := cm.reader.ReadPacket(c.netConn, c.version)
	if err != nil {
		if errors.Is(err, io.EOF) {
			cm.log.Debug().
				Str("ClientId", string(c.clientID)).
				Bool("Connected", c.connected).
				Int("Timeout", c.timeout).
				Int("Version", int(c.version)).
				Msg("Network connection was closed: " + err.Error())
			return nil, io.EOF
		}

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			cm.log.Debug().
				Str("ClientId", string(c.clientID)).
				Bool("Connected", c.connected).
				Int("Timeout", c.timeout).
				Int("Version", int(c.version)).
				Msg("Timeout - No packet received")
			return nil, errConnectionTimeout
		}

		cm.log.Error().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg("Failed to read packet: " + err.Error())
		return nil, errProtocolError
	}

	cm.metrics.recordPacketReceived(p)
	cm.log.Debug().
		Str("ClientId", string(c.clientID)).
		Bool("Connected", c.connected).
		Uint8("PacketTypeId", uint8(p.Type())).
		Int("Size", p.Size()).
		Int("Version", int(c.version)).
		Msg("Received packet")
	return p, nil
}

func (cm *connectionManager) handlePacket(c *connection, p packet.Packet) error {
	var replies []packet.Packet
	var err error

	h, ok := cm.handlers[p.Type()]
	if ok {
		replies, err = h.HandlePacket(c.clientID, p)
	} else {
		err = errors.New("invalid packet type")
	}
	if err != nil {
		cm.log.Error().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected).
			Bool("HasSession", c.hasSession).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg(fmt.Sprintf("failed to handle packet %v: %v",
				p.Type().String(),
				err.Error()))
	}

	for _, reply := range replies {
		if reply.Type() == packet.CONNACK {
			ack := reply.(*packet.ConnAck)
			if ack.ReasonCode == packet.ReasonCodeSuccess {
				c.clientID = ack.ClientID
				c.version = ack.Version
				c.timeout = ack.KeepAlive
				c.connected = true
				c.hasSession = true
				cm.log.Debug().
					Str("ClientId", string(c.clientID)).
					Bool("Connected", c.connected).
					Bool("HasSession", c.hasSession).
					Int("Timeout", c.timeout).
					Int("Version", int(c.version)).
					Msg("New connection")

				cm.mutex.Lock()
				cm.connections[c.clientID] = c
				cm.mutex.Unlock()
			}
		}

		if rplErr := cm.replyPacket(reply, c); rplErr != nil {
			err = multierr.Combine(err, rplErr)
			return err
		}

		cm.recordLatencyMetrics(p, reply)
		cm.metrics.recordPacketSent(reply)
		cm.log.Debug().
			Str("ClientId", string(c.clientID)).
			Uint8("PacketTypeId", uint8(reply.Type())).
			Int("Size", reply.Size()).
			Uint8("Version", uint8(c.version)).
			Msg("Packet sent with success")

		if reply.Type() == packet.DISCONNECT {
			c.hasSession = false
			cm.closeConnection(c, false /*force*/)
			break
		}
	}
	if err != nil {
		return err
	}

	if p.Type() == packet.DISCONNECT {
		c.hasSession = false
		cm.closeConnection(c, false /*force*/)

		latency := time.Since(p.Timestamp())
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

func (cm *connectionManager) disconnectSession(c *connection) {
	s, err := cm.sessionStore.ReadSession(c.clientID)
	if err != nil {
		cm.log.Error().
			Str("ClientId", string(c.clientID)).
			Bool("Connected", c.connected).
			Bool("HasSession", c.hasSession).
			Int("Timeout", c.timeout).
			Int("Version", int(c.version)).
			Msg("Failed to read session (CONNMGR): " + err.Error())
		return
	}

	if s.CleanSession {
		err = cm.sessionStore.DeleteSession(s)
		if err != nil {
			cm.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("Failed to delete session (CONNMGR): " + err.Error())
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
				Msg("Failed to save session (CONNMGR): " + err.Error())
			return
		}
	}
	c.hasSession = false
}

func (cm *connectionManager) closeConnection(c *connection, force bool) {
	if !c.connected && !c.hasSession {
		return
	}

	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Bool("Force", force).
		Msg("Closing connection")

	if tcp, ok := c.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0 /*sec*/)
	}

	_ = c.netConn.Close()

	if c.connected {
		cm.mutex.Lock()
		delete(cm.connections, c.clientID)
		c.connected = false
		cm.mutex.Unlock()
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

func (cm *connectionManager) replyPacket(rpl packet.Packet, c *connection) error {
	cm.log.Trace().
		Str("ClientId", string(c.clientID)).
		Uint8("PacketTypeId", uint8(rpl.Type())).
		Int("Size", rpl.Size()).
		Uint8("Version", uint8(c.version)).
		Msg("Sending packet")

	err := cm.writer.WritePacket(c.netConn, rpl)
	if err != nil {
		cm.log.Warn().
			Str("ClientId", string(c.clientID)).
			Stringer("PacketType", rpl.Type()).
			Uint8("Version", uint8(c.version)).
			Msg("Failed to send packet: " + err.Error())
		return fmt.Errorf("failed to send packet: %w", err)
	}
	return nil
}

func (cm *connectionManager) deliverPacket(id packet.ClientID, p *packet.Publish) error {
	cm.mutex.RLock()
	conn, ok := cm.connections[id]
	cm.mutex.RUnlock()
	if !ok {
		return errors.New("connection not found")
	}

	cm.log.Trace().
		Str("ClientId", string(conn.clientID)).
		Uint16("PacketId", uint16(p.PacketID)).
		Uint8("QoS", uint8(p.QoS)).
		Uint8("Retain", p.Retain).
		Int("Size", p.Size()).
		Str("TopicName", p.TopicName).
		Uint8("Version", uint8(p.Version)).
		Msg("Delivering packet to client")

	err := cm.writer.WritePacket(conn.netConn, p)
	if err != nil {
		return err
	}

	cm.log.Debug().
		Str("ClientId", string(conn.clientID)).
		Uint16("PacketId", uint16(p.PacketID)).
		Uint8("QoS", uint8(p.QoS)).
		Uint8("Retain", p.Retain).
		Int("Size", p.Size()).
		Str("TopicName", p.TopicName).
		Uint8("Version", uint8(p.Version)).
		Msg("Packet delivered to client with success")
	return nil
}

func (cm *connectionManager) recordLatencyMetrics(pkt packet.Packet, reply packet.Packet) {
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
