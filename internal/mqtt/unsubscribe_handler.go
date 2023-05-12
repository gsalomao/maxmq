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

type unsubscribeHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
	pubSub       *pubSub
}

func newUnsubscribeHandler(ss *sessionStore, ps *pubSub, l *logger.Logger) *unsubscribeHandler {
	return &unsubscribeHandler{
		log:          l.WithPrefix("mqtt.unsubscribe"),
		sessionStore: ss,
		pubSub:       ps,
	}
}

func (h *unsubscribeHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	unsub := p.(*packet.Unsubscribe)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(unsub.PacketID)).
		Int("Topics", len(unsub.Topics)).
		Uint8("Version", uint8(unsub.Version)).
		Msg("Received UNSUBSCRIBE packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(unsub.Version)).
			Msg("Failed to read session (UNSUBSCRIBE): " + err.Error())
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	codes, changed := h.unsubscribe(s, unsub)
	if changed {
		err = h.sessionStore.saveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to save session (UNSUBSCRIBE): " + err.Error())
			for i := 0; i < len(codes); i++ {
				codes[i] = packet.ReasonCodeV5UnspecifiedError
			}
		}
	}

	replies = make([]packet.Packet, 0, 1)

	unsubAck := packet.NewUnsubAck(unsub.PacketID, s.version, codes, nil)
	replies = append(replies, &unsubAck)
	h.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint16("PacketId", uint16(unsubAck.PacketID)).
		Int("ReasonCodes", len(unsubAck.ReasonCodes)).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Uint8("Version", uint8(unsubAck.Version)).
		Msg("Sending UNSUBACK packet")
	return replies, nil
}

func (h *unsubscribeHandler) unsubscribe(s *session, p *packet.Unsubscribe) (codes []packet.ReasonCode, changed bool) {
	codes = make([]packet.ReasonCode, 0, len(p.Topics))

	for _, topic := range p.Topics {
		err := h.pubSub.unsubscribe(s.clientID, topic)
		if err != nil {
			var code packet.ReasonCode
			if err != errSubscriptionNotFound {
				code = packet.ReasonCodeV5UnspecifiedError
				msg := err.Error()
				h.log.Error().
					Str("ClientId", string(s.clientID)).
					Uint64("SessionId", uint64(s.sessionID)).
					Uint8("Version", uint8(s.version)).
					Msg("Failed to unsubscribe (UNSUBSCRIBE): " + msg)
			} else {
				code = packet.ReasonCodeV5NoSubscriptionExisted
				h.log.Warn().
					Str("ClientId", string(s.clientID)).
					Uint64("SessionId", uint64(s.sessionID)).
					Str("TopicFilter", topic).
					Uint8("Version", uint8(s.version)).
					Msg("Subscription not found (UNSUBSCRIBE)")
			}
			codes = append(codes, code)
			continue
		}

		delete(s.subscriptions, topic)
		changed = true

		h.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint16("PacketId", uint16(p.PacketID)).
			Uint64("SessionId", uint64(s.sessionID)).
			Int("Subscriptions", len(s.subscriptions)).
			Str("TopicFilter", topic).
			Int("Topics", len(p.Topics)).
			Uint8("Version", uint8(s.version)).
			Msg("Client unsubscribed to topic")
		codes = append(codes, packet.ReasonCodeV5Success)
	}

	return codes, changed
}
