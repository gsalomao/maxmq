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

// UnsubscribeHandler is responsible for handling UNSUBSCRIBE packets.
type UnsubscribeHandler struct {
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
}

// NewUnsubscribeHandler creates a new UnsubscribeHandler.
func NewUnsubscribeHandler(
	sm SessionStore, subMgr SubscriptionManager, l *logger.Logger,
) *UnsubscribeHandler {
	return &UnsubscribeHandler{
		log:             l,
		sessionStore:    sm,
		subscriptionMgr: subMgr,
	}
}

// HandlePacket handles the given packet as a UNSUBSCRIBE packet.
func (h *UnsubscribeHandler) HandlePacket(
	id packet.ClientID, p packet.Packet,
) ([]packet.Packet, error) {
	unsub := p.(*packet.Unsubscribe)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(unsub.PacketID)).
		Int("Topics", len(unsub.Topics)).
		Uint8("Version", uint8(unsub.Version)).
		Msg("MQTT Received UNSUBSCRIBE packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(unsub.Version)).
			Msg("MQTT Failed to read session (UNSUBSCRIBE): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	codes, sessionChanged := h.unsubscribe(s, unsub)
	if sessionChanged {
		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to save session (UNSUBSCRIBE): " + err.Error())
			for i := 0; i < len(codes); i++ {
				codes[i] = packet.ReasonCodeV5UnspecifiedError
			}
		}
	}

	replies := make([]packet.Packet, 0, 1)

	unsubAck := packet.NewUnsubAck(unsub.PacketID, s.Version, codes, nil)
	replies = append(replies, &unsubAck)
	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint16("PacketId", uint16(unsubAck.PacketID)).
		Int("ReasonCodes", len(unsubAck.ReasonCodes)).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Uint8("Version", uint8(unsubAck.Version)).
		Msg("MQTT Sending UNSUBACK packet")
	return replies, nil
}

func (h *UnsubscribeHandler) unsubscribe(
	s *Session, p *packet.Unsubscribe,
) ([]packet.ReasonCode, bool) {
	codes := make([]packet.ReasonCode, 0, len(p.Topics))
	var sessionChanged bool

	for _, topic := range p.Topics {
		err := h.subscriptionMgr.Unsubscribe(s.ClientID, topic)
		if err != nil {
			var code packet.ReasonCode
			if err != ErrSubscriptionNotFound {
				code = packet.ReasonCodeV5UnspecifiedError
				msg := err.Error()
				h.log.Error().
					Str("ClientId", string(s.ClientID)).
					Uint64("SessionId", uint64(s.SessionID)).
					Uint8("Version", uint8(s.Version)).
					Msg("MQTT Failed to unsubscribe (UNSUBSCRIBE): " + msg)
			} else {
				code = packet.ReasonCodeV5NoSubscriptionExisted
				h.log.Warn().
					Str("ClientId", string(s.ClientID)).
					Uint64("SessionId", uint64(s.SessionID)).
					Str("TopicFilter", topic).
					Uint8("Version", uint8(s.Version)).
					Msg("MQTT Subscription not found (UNSUBSCRIBE)")
			}
			codes = append(codes, code)
			continue
		}

		delete(s.Subscriptions, topic)
		sessionChanged = true

		h.log.Info().
			Str("ClientId", string(s.ClientID)).
			Uint16("PacketId", uint16(p.PacketID)).
			Uint64("SessionId", uint64(s.SessionID)).
			Int("Subscriptions", len(s.Subscriptions)).
			Str("TopicFilter", topic).
			Int("Topics", len(p.Topics)).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Client unsubscribed to topic")
		codes = append(codes, packet.ReasonCodeV5Success)
	}

	return codes, sessionChanged
}
