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
	"math"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/rs/xid"
)

type connectHandler struct {
	conf         Config
	log          *logger.Logger
	sessionStore *sessionStore
	props        []packet.UserProperty
}

func newConnectHandler(c Config, ss *sessionStore, l *logger.Logger) *connectHandler {
	props := make([]packet.UserProperty, 0, len(c.UserProperties))
	for k, v := range c.UserProperties {
		prop := packet.UserProperty{Key: []byte(k), Value: []byte(v)}
		props = append(props, prop)
	}

	return &connectHandler{
		conf:         c,
		log:          l.WithPrefix("mqtt.connect"),
		props:        props,
		sessionStore: ss,
	}
}

func (h *connectHandler) handlePacket(_ packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	connPkt := p.(*packet.Connect)
	h.log.Trace().
		Bool("CleanSession", connPkt.CleanSession).
		Bytes("ClientId", connPkt.ClientID).
		Uint16("KeepAlive", connPkt.KeepAlive).
		Uint8("Version", uint8(connPkt.Version)).
		Msg("Received CONNECT packet")

	if pktErr := h.checkPacket(connPkt); pktErr != nil {
		h.log.Trace().
			Str("ClientId", string(connPkt.ClientID)).
			Int("ReasonCode", int(pktErr.ReasonCode)).
			Uint8("Version", uint8(connPkt.Version)).
			Msg("Sending CONNACK packet (Error)")
		connAck := h.newConnAck(connPkt, nil, pktErr.ReasonCode, nil)
		return []packet.Packet{connAck}, pktErr
	}

	var idGenerated bool
	id := packet.ClientID(connPkt.ClientID)
	if len(id) == 0 {
		id = h.createClientID()
		idGenerated = true
	}

	var s *session
	if !idGenerated {
		s, err = h.sessionStore.readSession(id)
		if err != nil && err != errSessionNotFound {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("Version", uint8(connPkt.Version)).
				Msg("Failed to read session (CONNECT): " + err.Error())
			return h.replyUnavailable(connPkt, id), err
		}
	}

	if s != nil && connPkt.CleanSession {
		err = h.sessionStore.deleteSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to delete session (CONNECT): " + err.Error())
			return h.replyUnavailable(connPkt, id), err
		}
		s = nil
	}
	if s == nil {
		s = h.sessionStore.newSession(id)
		s.keepAlive = h.conf.ConnectTimeout
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.clientID = id
	s.clientIDGenerated = idGenerated
	s.version = connPkt.Version
	s.cleanSession = connPkt.CleanSession
	s.expiryInterval = h.sessionExpiryInterval(connPkt)
	s.connectedAt = time.Now().UnixMilli()
	s.connected = true
	s.keepAlive = h.sessionKeepAlive(int(connPkt.KeepAlive))
	h.log.Info().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("New MQTT connection")

	code := packet.ReasonCodeV3ConnectionAccepted
	connAckPkt := h.newConnAck(connPkt, s, code, nil)

	replies = make([]packet.Packet, 0, 1)
	h.log.Trace().
		Str("ClientId", string(connAckPkt.ClientID)).
		Int("KeepAlive", connAckPkt.KeepAlive).
		Uint8("ReasonCode", byte(connAckPkt.ReasonCode)).
		Uint64("SessionId", uint64(s.sessionID)).
		Bool("SessionPresent", connAckPkt.SessionPresent).
		Uint8("Version", uint8(connAckPkt.Version)).
		Msg("Sending CONNACK packet")

	replies = append(replies, connAckPkt)
	h.sendInflightMessages(&replies, s)

	err = h.sessionStore.saveSession(s)
	if err != nil {
		h.log.Error().
			Bytes("ClientId", connPkt.ClientID).
			Uint64("SessionId", uint64(s.sessionID)).
			Uint8("Version", uint8(s.version)).
			Msg("Failed to save session (CONNECT): " + err.Error())
		return h.replyUnavailable(connPkt, s.clientID), err
	}

	return replies, nil
}

func (h *connectHandler) checkPacket(p *packet.Connect) *packet.Error {
	if err := h.checkKeepAlive(p); err != nil {
		return err
	}
	if err := h.checkClientID(p); err != nil {
		return err
	}
	return nil
}

func (h *connectHandler) checkKeepAlive(p *packet.Connect) *packet.Error {
	// For V5.0 clients, the Keep Alive sent in the CONNECT Packet can be overwritten in the CONNACK Packet.
	if p.Version == packet.MQTT50 || h.conf.MaxKeepAlive == 0 {
		return nil
	}

	keepAlive := int(p.KeepAlive)
	if keepAlive == 0 || keepAlive > h.conf.MaxKeepAlive {
		// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
		// clients what Keep Alive value they should use. If an MQTT
		// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
		// MaxKeepAlive, the CONNACK Packet shall be sent with the reason code
		// "identifier rejected".
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (h *connectHandler) checkClientID(p *packet.Connect) *packet.Error {
	idLen := len(p.ClientID)
	if idLen == 0 && !h.conf.AllowEmptyClientID {
		if p.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	if p.Version == packet.MQTT31 && idLen > 23 {
		return packet.ErrV3IdentifierRejected
	}

	if idLen > h.conf.MaxClientIDLen {
		if p.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (h *connectHandler) createClientID() packet.ClientID {
	guid := xid.New()
	prefix := h.conf.ClientIDPrefix
	prefixLen := len(prefix)
	guidEncodedLen := 20
	id := make([]byte, prefixLen+guidEncodedLen)

	if prefixLen > 0 {
		_ = copy(id, prefix)
	}

	_ = guid.Encode(id[prefixLen:])
	return packet.ClientID(id)
}

func (h *connectHandler) sessionExpiryInterval(p *packet.Connect) uint32 {
	interval := uint32(math.MaxUint32)
	if p.Version == packet.MQTT50 {
		if p.Properties == nil || p.Properties.SessionExpiryInterval == nil {
			return 0
		}
		interval = *p.Properties.SessionExpiryInterval
		maxExp := h.conf.MaxSessionExpiryInterval
		if maxExp > 0 && interval > maxExp {
			interval = maxExp
		}
	}
	return interval
}

func (h *connectHandler) sessionKeepAlive(keepAlive int) int {
	if h.conf.MaxKeepAlive > 0 && (keepAlive == 0 || keepAlive > h.conf.MaxKeepAlive) {
		return h.conf.MaxKeepAlive
	}
	return keepAlive
}

func (h *connectHandler) sendInflightMessages(r *[]packet.Packet, s *session) {
	inflightMsg := s.inflightMessages.Front()
	for inflightMsg != nil {
		msg := inflightMsg.Value.(*message)
		if msg.packet != nil {
			msg.packet.Version = s.version
			msg.tries++
			msg.lastSent = time.Now().UnixMicro()
			*r = append(*r, msg.packet)
			h.log.Trace().
				Str("ClientId", string(s.clientID)).
				Int("InflightMessages", s.inflightMessages.Len()).
				Uint16("PacketId", uint16(msg.packet.PacketID)).
				Uint8("QoS", uint8(msg.packet.QoS)).
				Int("Replies", len(*r)).
				Uint8("Retain", msg.packet.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", msg.packet.TopicName).
				Int("UnAckMessages", len(s.unAckMessages)).
				Uint8("Version", uint8(msg.packet.Version)).
				Msg("Sending PUBLISH packet")
		}
		inflightMsg = inflightMsg.Next()
	}
}

func (h *connectHandler) replyUnavailable(p *packet.Connect, id packet.ClientID) []packet.Packet {
	code := packet.ReasonCodeV5ServerUnavailable
	if p.Version != packet.MQTT50 {
		code = packet.ReasonCodeV3ServerUnavailable
	}
	h.log.Warn().
		Str("ClientId", string(id)).
		Int("ReasonCode", int(code)).
		Uint8("Version", uint8(p.Version)).
		Msg("Sending CONNACK packet (Unavailable)")

	connAck := h.newConnAck(p, nil, code, nil)
	return []packet.Packet{connAck}
}

func (h *connectHandler) newConnAck(pkt *packet.Connect, s *session, c packet.ReasonCode,
	p *packet.Properties) *packet.ConnAck {

	var id packet.ClientID
	var keepAlive int
	var sessionPresent bool

	if s != nil {
		id = s.clientID
		keepAlive = s.keepAlive
		sessionPresent = s.restored

		if s.version == packet.MQTT50 && c == packet.ReasonCodeV5Success {
			var expInterval *uint32

			if pkt.Properties != nil {
				expInterval = pkt.Properties.SessionExpiryInterval
			}

			h.addAssignedClientID(&p, s)
			h.addSessionExpiryInterval(&p, expInterval)
			h.addServerKeepAlive(&p, s, int(pkt.KeepAlive))
			h.addReceiveMaximum(&p)
			h.addMaxPacketSize(&p)
			h.addMaximumQoS(&p)
			h.addTopicAliasMax(&p)
			h.addRetainAvailable(&p)
			h.addWildcardSubsAvailable(&p)
			h.addSubscriptionIDAvailable(&p)
			h.addSharedSubsAvailable(&p)
			h.addUserProperties(&p)
		}
	}

	connAckPkt := packet.NewConnAck(id, pkt.Version, c, sessionPresent, keepAlive, p)
	return &connAckPkt
}

func (h *connectHandler) addAssignedClientID(p **packet.Properties, s *session) {
	if s.clientIDGenerated {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).AssignedClientID = []byte(s.clientID)
	}
}

func (h *connectHandler) addSessionExpiryInterval(p **packet.Properties, interval *uint32) {
	if interval == nil {
		return
	}

	maxExpInt := h.conf.MaxSessionExpiryInterval
	if maxExpInt > 0 && *interval > maxExpInt {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).SessionExpiryInterval = new(uint32)
		*(*p).SessionExpiryInterval = h.conf.MaxSessionExpiryInterval
	}
}

func (h *connectHandler) addServerKeepAlive(p **packet.Properties, s *session, keepAlive int) {
	if keepAlive != s.keepAlive {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).ServerKeepAlive = new(uint16)
		*(*p).ServerKeepAlive = uint16(s.keepAlive)
	}
}

func (h *connectHandler) addReceiveMaximum(p **packet.Properties) {
	if h.conf.MaxInflightMessages > 0 && h.conf.MaxInflightMessages < 65535 {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).ReceiveMaximum = new(uint16)
		*(*p).ReceiveMaximum = uint16(h.conf.MaxInflightMessages)
	}
}

func (h *connectHandler) addMaxPacketSize(p **packet.Properties) {
	if h.conf.MaxPacketSize > 0 && h.conf.MaxPacketSize < 268435456 {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).MaximumPacketSize = new(uint32)
		*(*p).MaximumPacketSize = uint32(h.conf.MaxPacketSize)
	}
}

func (h *connectHandler) addMaximumQoS(p **packet.Properties) {
	if h.conf.MaximumQoS < 2 {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).MaximumQoS = new(byte)
		*(*p).MaximumQoS = byte(h.conf.MaximumQoS)
	}
}

func (h *connectHandler) addTopicAliasMax(p **packet.Properties) {
	if h.conf.MaxTopicAlias > 0 {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).TopicAliasMaximum = new(uint16)
		*(*p).TopicAliasMaximum = uint16(h.conf.MaxTopicAlias)
	}
}

func (h *connectHandler) addRetainAvailable(p **packet.Properties) {
	if !h.conf.RetainAvailable {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).RetainAvailable = new(byte)
		*(*p).RetainAvailable = 0
	}
}

func (h *connectHandler) addWildcardSubsAvailable(p **packet.Properties) {
	if !h.conf.WildcardSubscriptionAvailable {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).WildcardSubscriptionAvailable = new(byte)
		*(*p).WildcardSubscriptionAvailable = 0
	}
}

func (h *connectHandler) addSubscriptionIDAvailable(p **packet.Properties) {
	if !h.conf.SubscriptionIDAvailable {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).SubscriptionIDAvailable = new(byte)
		*(*p).SubscriptionIDAvailable = 0
	}
}

func (h *connectHandler) addSharedSubsAvailable(p **packet.Properties) {
	if !h.conf.SharedSubscriptionAvailable {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).SharedSubscriptionAvailable = new(byte)
		*(*p).SharedSubscriptionAvailable = 0
	}
}

func (h *connectHandler) addUserProperties(p **packet.Properties) {
	if len(h.conf.UserProperties) > 0 {
		if *p == nil {
			*p = &packet.Properties{}
		}
		(*p).UserProperties = h.props
	}
}
