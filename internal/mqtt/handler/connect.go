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

package handler

import (
	"math"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/rs/xid"
)

// ConnectHandler is responsible for handling CONNECT packets.
type ConnectHandler struct {
	// Unexported fields
	conf         *Configuration
	log          *logger.Logger
	props        []packet.UserProperty
	sessionStore SessionStore
}

// NewConnectHandler creates a new ConnectHandler.
func NewConnectHandler(c *Configuration, st SessionStore, l *logger.Logger) *ConnectHandler {
	props := make([]packet.UserProperty, 0, len(c.UserProperties))
	for k, v := range c.UserProperties {
		prop := packet.UserProperty{Key: []byte(k), Value: []byte(v)}
		props = append(props, prop)
	}

	return &ConnectHandler{conf: c, log: l, props: props, sessionStore: st}
}

// HandlePacket handles the given packet as CONNECT packet.
func (h *ConnectHandler) HandlePacket(
	_ packet.ClientID,
	p packet.Packet,
) ([]packet.Packet, error) {
	connPkt := p.(*packet.Connect)
	h.log.Trace().
		Bool("CleanSession", connPkt.CleanSession).
		Bytes("ClientId", connPkt.ClientID).
		Uint16("KeepAlive", connPkt.KeepAlive).
		Uint8("Version", uint8(connPkt.Version)).
		Msg("MQTT Received CONNECT packet")

	if err := h.checkPacket(connPkt); err != nil {
		h.log.Trace().
			Str("ClientId", string(connPkt.ClientID)).
			Int("ReasonCode", int(err.ReasonCode)).
			Uint8("Version", uint8(connPkt.Version)).
			Msg("MQTT Sending CONNACK packet (Error)")
		connAck := h.newConnAck(connPkt, nil /*session*/, err.ReasonCode, nil /*props*/)
		return []packet.Packet{connAck}, err
	}

	var s *Session
	var err error
	var idGenerated bool

	id := packet.ClientID(connPkt.ClientID)
	if len(id) == 0 {
		id = h.createClientID()
		idGenerated = true
	}
	if !idGenerated {
		s, err = h.sessionStore.ReadSession(id)
		if err != nil && err != ErrSessionNotFound {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("Version", uint8(connPkt.Version)).
				Msg("MQTT Failed to read session (CONNECT): " + err.Error())
			return h.replyUnavailable(connPkt, id), err
		}
	}

	if s != nil && connPkt.CleanSession {
		err = h.sessionStore.DeleteSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to delete session (CONNECT): " + err.Error())
			return h.replyUnavailable(connPkt, id), err
		}
		s = nil
	}
	if s == nil {
		s = h.sessionStore.NewSession(id)
		s.KeepAlive = h.conf.ConnectTimeout
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.ClientID = id
	s.ClientIDGenerated = idGenerated
	s.Version = connPkt.Version
	s.CleanSession = connPkt.CleanSession
	s.ExpiryInterval = h.sessionExpiryInterval(connPkt)
	s.ConnectedAt = time.Now().UnixMilli()
	s.Connected = true
	s.KeepAlive = h.sessionKeepAlive(int(connPkt.KeepAlive))
	h.log.Info().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Int("InflightMessages", s.InflightMessages.Len()).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("MQTT Client connected")

	code := packet.ReasonCodeV3ConnectionAccepted
	connAckPkt := h.newConnAck(connPkt, s, code, nil /*props*/)

	replies := make([]packet.Packet, 0, 1)
	h.log.Trace().
		Str("ClientId", string(connAckPkt.ClientID)).
		Int("KeepAlive", connAckPkt.KeepAlive).
		Uint8("ReasonCode", byte(connAckPkt.ReasonCode)).
		Uint64("SessionId", uint64(s.SessionID)).
		Bool("SessionPresent", connAckPkt.SessionPresent).
		Uint8("Version", uint8(connAckPkt.Version)).
		Msg("MQTT Sending CONNACK packet")

	replies = append(replies, connAckPkt)
	h.addInflightMessages(&replies, s)

	err = h.sessionStore.SaveSession(s)
	if err != nil {
		h.log.Error().
			Bytes("ClientId", connPkt.ClientID).
			Uint64("SessionId", uint64(s.SessionID)).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Failed to save session (CONNECT): " + err.Error())
		return h.replyUnavailable(connPkt, s.ClientID), err
	}

	return replies, nil
}

func (h *ConnectHandler) checkPacket(p *packet.Connect) *packet.Error {
	if err := h.checkKeepAlive(p); err != nil {
		return err
	}
	if err := h.checkClientID(p); err != nil {
		return err
	}
	return nil
}

func (h *ConnectHandler) checkKeepAlive(p *packet.Connect) *packet.Error {
	// For V5.0 clients, the Keep Alive sent in the CONNECT Packet can be
	// overwritten in the CONNACK Packet.
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

func (h *ConnectHandler) checkClientID(p *packet.Connect) *packet.Error {
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

func (h *ConnectHandler) createClientID() packet.ClientID {
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

func (h *ConnectHandler) sessionExpiryInterval(p *packet.Connect) uint32 {
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

func (h *ConnectHandler) sessionKeepAlive(keepAlive int) int {
	if h.conf.MaxKeepAlive > 0 && (keepAlive == 0 || keepAlive > h.conf.MaxKeepAlive) {
		return h.conf.MaxKeepAlive
	}
	return keepAlive
}

func (h *ConnectHandler) addInflightMessages(r *[]packet.Packet, s *Session) {
	inflightMsg := s.InflightMessages.Front()
	for inflightMsg != nil {
		msg := inflightMsg.Value.(*Message)
		if msg.Packet != nil {
			msg.Packet.Version = s.Version
			msg.Tries++
			msg.LastSent = time.Now().UnixMicro()
			*r = append(*r, msg.Packet)
			h.log.Trace().
				Str("ClientId", string(s.ClientID)).
				Int("InflightMessages", s.InflightMessages.Len()).
				Uint16("PacketId", uint16(msg.Packet.PacketID)).
				Uint8("QoS", uint8(msg.Packet.QoS)).
				Int("Replies", len(*r)).
				Uint8("Retain", msg.Packet.Retain).
				Uint64("SessionId", uint64(s.SessionID)).
				Str("TopicName", msg.Packet.TopicName).
				Int("UnAckMessages", len(s.UnAckMessages)).
				Uint8("Version", uint8(msg.Packet.Version)).
				Msg("MQTT Adding PUBLISH packet")
		}
		inflightMsg = inflightMsg.Next()
	}
}

func (h *ConnectHandler) replyUnavailable(p *packet.Connect, id packet.ClientID) []packet.Packet {
	code := packet.ReasonCodeV5ServerUnavailable
	if p.Version != packet.MQTT50 {
		code = packet.ReasonCodeV3ServerUnavailable
	}
	h.log.Warn().
		Str("ClientId", string(id)).
		Int("ReasonCode", int(code)).
		Uint8("Version", uint8(p.Version)).
		Msg("MQTT Sending CONNACK packet (Unavailable)")

	connAck := h.newConnAck(p, nil /*session*/, code, nil /*props*/)
	return []packet.Packet{connAck}
}

func (h *ConnectHandler) newConnAck(
	p *packet.Connect, s *Session, code packet.ReasonCode, props *packet.Properties,
) *packet.ConnAck {
	var id packet.ClientID
	var keepAlive int
	var sessionPresent bool

	if s != nil {
		id = s.ClientID
		keepAlive = s.KeepAlive
		sessionPresent = s.Restored

		if s.Version == packet.MQTT50 && code == packet.ReasonCodeV5Success {
			var expInterval *uint32

			if p.Properties != nil {
				expInterval = p.Properties.SessionExpiryInterval
			}

			h.addAssignedClientID(&props, s)
			h.addSessionExpiryInterval(&props, expInterval)
			h.addServerKeepAlive(&props, s, int(p.KeepAlive))
			h.addReceiveMaximum(&props)
			h.addMaxPacketSize(&props)
			h.addMaximumQoS(&props)
			h.addTopicAliasMax(&props)
			h.addRetainAvailable(&props)
			h.addWildcardSubsAvailable(&props)
			h.addSubscriptionIDAvailable(&props)
			h.addSharedSubsAvailable(&props)
			h.addUserProperties(&props)
		}
	}

	connAckPkt := packet.NewConnAck(id, p.Version, code, sessionPresent, keepAlive, props)
	return &connAckPkt
}

func (h *ConnectHandler) addAssignedClientID(props **packet.Properties, s *Session) {
	if s.ClientIDGenerated {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).AssignedClientID = []byte(s.ClientID)
	}
}

func (h *ConnectHandler) addSessionExpiryInterval(props **packet.Properties, interval *uint32) {
	if interval == nil {
		return
	}

	maxExpInt := h.conf.MaxSessionExpiryInterval
	if maxExpInt > 0 && *interval > maxExpInt {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).SessionExpiryInterval = new(uint32)
		*(*props).SessionExpiryInterval = h.conf.MaxSessionExpiryInterval
	}
}

func (h *ConnectHandler) addServerKeepAlive(props **packet.Properties, s *Session, keepAlive int) {
	if keepAlive != s.KeepAlive {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).ServerKeepAlive = new(uint16)
		*(*props).ServerKeepAlive = uint16(s.KeepAlive)
	}
}

func (h *ConnectHandler) addReceiveMaximum(props **packet.Properties) {
	if h.conf.MaxInflightMessages > 0 && h.conf.MaxInflightMessages < 65535 {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).ReceiveMaximum = new(uint16)
		*(*props).ReceiveMaximum = uint16(h.conf.MaxInflightMessages)
	}
}

func (h *ConnectHandler) addMaxPacketSize(props **packet.Properties) {
	if h.conf.MaxPacketSize > 0 && h.conf.MaxPacketSize < 268435456 {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).MaximumPacketSize = new(uint32)
		*(*props).MaximumPacketSize = uint32(h.conf.MaxPacketSize)
	}
}

func (h *ConnectHandler) addMaximumQoS(props **packet.Properties) {
	if h.conf.MaximumQoS < 2 {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).MaximumQoS = new(byte)
		*(*props).MaximumQoS = byte(h.conf.MaximumQoS)
	}
}

func (h *ConnectHandler) addTopicAliasMax(props **packet.Properties) {
	if h.conf.MaxTopicAlias > 0 {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).TopicAliasMaximum = new(uint16)
		*(*props).TopicAliasMaximum = uint16(h.conf.MaxTopicAlias)
	}
}

func (h *ConnectHandler) addRetainAvailable(props **packet.Properties) {
	if !h.conf.RetainAvailable {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).RetainAvailable = new(byte)
		*(*props).RetainAvailable = 0
	}
}

func (h *ConnectHandler) addWildcardSubsAvailable(props **packet.Properties) {
	if !h.conf.WildcardSubscriptionAvailable {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).WildcardSubscriptionAvailable = new(byte)
		*(*props).WildcardSubscriptionAvailable = 0
	}
}

func (h *ConnectHandler) addSubscriptionIDAvailable(props **packet.Properties) {
	if !h.conf.SubscriptionIDAvailable {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).SubscriptionIDAvailable = new(byte)
		*(*props).SubscriptionIDAvailable = 0
	}
}

func (h *ConnectHandler) addSharedSubsAvailable(props **packet.Properties) {
	if !h.conf.SharedSubscriptionAvailable {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).SharedSubscriptionAvailable = new(byte)
		*(*props).SharedSubscriptionAvailable = 0
	}
}

func (h *ConnectHandler) addUserProperties(props **packet.Properties) {
	if len(h.conf.UserProperties) > 0 {
		if *props == nil {
			*props = &packet.Properties{}
		}
		(*props).UserProperties = h.props
	}
}
