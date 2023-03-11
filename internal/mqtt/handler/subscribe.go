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
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// SubscribeHandler is responsible for handling SUBSCRIBE packets.
type SubscribeHandler struct {
	conf            *Configuration
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
}

// NewSubscribeHandler creates a new SubscribeHandler.
func NewSubscribeHandler(
	c *Configuration,
	st SessionStore,
	subMgr SubscriptionManager,
	l *logger.Logger,
) *SubscribeHandler {
	return &SubscribeHandler{conf: c, log: l, sessionStore: st, subscriptionMgr: subMgr}
}

// HandlePacket handles the given packet as SUBSCRIBE packet.
func (h *SubscribeHandler) HandlePacket(
	id packet.ClientID,
	p packet.Packet,
) ([]packet.Packet, error) {
	sub := p.(*packet.Subscribe)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(sub.PacketID)).
		Int("Topics", len(sub.Topics)).
		Uint8("Version", uint8(sub.Version)).
		Msg("MQTT Received SUBSCRIBE packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(sub.Version)).
			Msg("MQTT Failed to read session (SUBSCRIBE): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	subsID := sub.Properties.SubscriptionID()
	if subsID > 0 && !h.conf.SubscriptionIDAvailable {
		h.log.Info().
			Str("ClientId", string(s.ClientID)).
			Uint16("PacketId", uint16(sub.PacketID)).
			Uint64("SessionId", uint64(s.SessionID)).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Received SUBSCRIBE with subscription ID (not available)")
		code := packet.ReasonCodeV5SubscriptionIDNotSupported
		discPkt := packet.NewDisconnect(s.Version, code, nil)
		return []packet.Packet{&discPkt}, packet.ErrV5SubscriptionIDNotSupported
	}

	codes, sessionChanged := h.subscribe(s, sub, subsID)
	if sessionChanged {
		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to save session (SUBSCRIBE): " + err.Error())
			h.unsubscribe(codes, s, sub.Topics)
		}
	}

	subAck := packet.NewSubAck(sub.PacketID, s.Version, codes, nil /*props*/)
	replies := make([]packet.Packet, 0, 1)
	replies = append(replies, &subAck)
	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint16("PacketId", uint16(subAck.PacketID)).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Uint8("Version", uint8(subAck.Version)).
		Msg("MQTT Sending SUBACK packet")
	return replies, nil
}

func (h *SubscribeHandler) subscribe(
	s *Session,
	p *packet.Subscribe,
	subsID int,
) ([]packet.ReasonCode, bool) {
	var sessionChanged bool
	codes := make([]packet.ReasonCode, 0, len(p.Topics))

	for _, topic := range p.Topics {
		sub := &Subscription{
			ID:                subsID,
			ClientID:          s.ClientID,
			TopicFilter:       topic.Name,
			QoS:               topic.QoS,
			RetainHandling:    topic.RetainHandling,
			RetainAsPublished: topic.RetainAsPublished,
			NoLocal:           topic.NoLocal,
		}

		err := h.subscriptionMgr.Subscribe(sub)
		if err != nil {
			h.log.Warn().
				Str("ClientId", string(sub.ClientID)).
				Bool("NoLocal", sub.NoLocal).
				Uint16("PacketId", uint16(p.PacketID)).
				Uint8("QoS", byte(sub.QoS)).
				Bool("RetainAsPublished", sub.RetainAsPublished).
				Uint8("RetainHandling", sub.RetainHandling).
				Uint64("SessionId", uint64(s.SessionID)).
				Int("SubscriptionId", sub.ID).
				Int("Subscriptions", len(s.Subscriptions)).
				Str("TopicFilter", sub.TopicFilter).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to subscribe (SUBSCRIBE): " + err.Error())
			codes = append(codes, packet.ReasonCodeV3Failure)
			continue
		}

		codes = append(codes, packet.ReasonCode(sub.QoS))
		s.Subscriptions[sub.TopicFilter] = sub
		sessionChanged = true

		h.log.Info().
			Str("ClientId", string(s.ClientID)).
			Bool("NoLocal", sub.NoLocal).
			Uint16("PacketId", uint16(p.PacketID)).
			Uint8("QoS", byte(sub.QoS)).
			Bool("RetainAsPublished", sub.RetainAsPublished).
			Uint8("RetainHandling", sub.RetainHandling).
			Uint64("SessionId", uint64(s.SessionID)).
			Int("SubscriptionId", sub.ID).
			Int("Subscriptions", len(s.Subscriptions)).
			Str("TopicFilter", sub.TopicFilter).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Client subscribed to topic")
	}

	return codes, sessionChanged
}

func (h *SubscribeHandler) unsubscribe(
	codes []packet.ReasonCode,
	s *Session,
	topics []packet.Topic,
) {
	for idx, code := range codes {
		if code == packet.ReasonCodeV3Failure {
			continue
		}

		topic := topics[idx].Name
		h.log.Trace().
			Str("ClientId", string(s.ClientID)).
			Uint8("ReasonCode", byte(code)).
			Uint64("SessionId", uint64(s.SessionID)).
			Str("TopicFilter", topic).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Unsubscribing due to error (SUBSCRIBE)")

		err := h.subscriptionMgr.Unsubscribe(s.ClientID, topic)
		if err != nil {
			msg := err.Error()
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint8("ReasonCode", byte(code)).
				Uint64("SessionId", uint64(s.SessionID)).
				Str("TopicFilter", topic).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to unsubscribe (SUBSCRIBE): " + msg)
		}

		delete(s.Subscriptions, topic)
		codes[idx] = packet.ReasonCodeV3Failure
	}
}
