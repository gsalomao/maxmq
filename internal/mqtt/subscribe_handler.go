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
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type subscribeHandler struct {
	conf         Config
	log          *logger.Logger
	sessionStore *sessionStore
	pubSub       *pubSub
}

func newSubscribeHandler(c Config, ss *sessionStore, ps *pubSub, l *logger.Logger) *subscribeHandler {
	return &subscribeHandler{
		conf:         c,
		log:          l.WithPrefix("mqtt.subscribe"),
		sessionStore: ss,
		pubSub:       ps,
	}
}

func (h *subscribeHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	sub := p.(*packet.Subscribe)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(sub.PacketID)).
		Int("Topics", len(sub.Topics)).
		Uint8("Version", uint8(sub.Version)).
		Msg("Received SUBSCRIBE packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(sub.Version)).
			Msg("Failed to read session (SUBSCRIBE): " + err.Error())
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	subsID := sub.Properties.SubscriptionID()
	if subsID > 0 && !h.conf.SubscriptionIDAvailable {
		h.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint16("PacketId", uint16(sub.PacketID)).
			Uint64("SessionId", uint64(s.sessionID)).
			Uint8("Version", uint8(s.version)).
			Msg("Received SUBSCRIBE with subscription ID (not available)")
		code := packet.ReasonCodeV5SubscriptionIDNotSupported
		discPkt := packet.NewDisconnect(s.version, code, nil)
		return []packet.Packet{&discPkt}, packet.ErrV5SubscriptionIDNotSupported
	}

	codes, sessionChanged := h.subscribe(s, sub, subscriptionID(subsID))
	if sessionChanged {
		err = h.sessionStore.saveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to save session (SUBSCRIBE): " + err.Error())
			h.unsubscribe(codes, s, sub.Topics)
		}
	}

	subAck := packet.NewSubAck(sub.PacketID, s.version, codes, nil)
	replies = make([]packet.Packet, 0, 1)
	replies = append(replies, &subAck)
	h.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint16("PacketId", uint16(subAck.PacketID)).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Uint8("Version", uint8(subAck.Version)).
		Msg("Sending SUBACK packet")
	return replies, nil
}

func (h *subscribeHandler) subscribe(s *session, p *packet.Subscribe, id subscriptionID) (codes []packet.ReasonCode,
	changed bool) {

	codes = make([]packet.ReasonCode, 0, len(p.Topics))

	for _, topic := range p.Topics {
		sub := &subscription{
			id:                id,
			clientID:          s.clientID,
			topicFilter:       topic.Name,
			qos:               topic.QoS,
			retainHandling:    topic.RetainHandling,
			retainAsPublished: topic.RetainAsPublished,
			noLocal:           topic.NoLocal,
		}

		err := h.pubSub.subscribe(sub)
		if err != nil {
			h.log.Warn().
				Str("ClientId", string(sub.clientID)).
				Bool("NoLocal", sub.noLocal).
				Uint16("PacketId", uint16(p.PacketID)).
				Uint8("QoS", byte(sub.qos)).
				Bool("RetainAsPublished", sub.retainAsPublished).
				Uint8("RetainHandling", sub.retainHandling).
				Uint64("SessionId", uint64(s.sessionID)).
				Int("SubscriptionId", int(sub.id)).
				Int("Subscriptions", len(s.subscriptions)).
				Str("TopicFilter", sub.topicFilter).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to subscribe (SUBSCRIBE): " + err.Error())
			codes = append(codes, packet.ReasonCodeV3Failure)
			continue
		}

		codes = append(codes, packet.ReasonCode(sub.qos))
		s.subscriptions[sub.topicFilter] = sub
		changed = true

		h.log.Info().
			Str("ClientId", string(s.clientID)).
			Bool("NoLocal", sub.noLocal).
			Uint16("PacketId", uint16(p.PacketID)).
			Uint8("QoS", byte(sub.qos)).
			Bool("RetainAsPublished", sub.retainAsPublished).
			Uint8("RetainHandling", sub.retainHandling).
			Uint64("SessionId", uint64(s.sessionID)).
			Int("SubscriptionId", int(sub.id)).
			Int("Subscriptions", len(s.subscriptions)).
			Str("TopicFilter", sub.topicFilter).
			Uint8("Version", uint8(s.version)).
			Msg("Client subscribed to topic")
	}

	return codes, changed
}

func (h *subscribeHandler) unsubscribe(codes []packet.ReasonCode, s *session, topics []packet.Topic) {
	for idx, code := range codes {
		if code == packet.ReasonCodeV3Failure {
			continue
		}

		topic := topics[idx].Name
		h.log.Trace().
			Str("ClientId", string(s.clientID)).
			Uint8("ReasonCode", byte(code)).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicFilter", topic).
			Uint8("Version", uint8(s.version)).
			Msg("Unsubscribing due to error (SUBSCRIBE)")

		err := h.pubSub.unsubscribe(s.clientID, topic)
		if err != nil {
			msg := err.Error()
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint8("ReasonCode", byte(code)).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicFilter", topic).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to unsubscribe (SUBSCRIBE): " + msg)
		}

		delete(s.subscriptions, topic)
		codes[idx] = packet.ReasonCodeV3Failure
	}
}
