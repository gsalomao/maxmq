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

// Unsubscribe is responsible for handling UNSUBSCRIBE packets.
type Unsubscribe struct {
	// Unexported fields
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
}

// NewUnsubscribe creates a new Unsubscribe handler.
func NewUnsubscribe(ss SessionStore, sm SubscriptionManager, l *logger.Logger) *Unsubscribe {
	return &Unsubscribe{log: l, sessionStore: ss, subscriptionMgr: sm}
}

// HandlePacket handles the given packet as UNSUBSCRIBE packet.
func (h *Unsubscribe) HandlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	unsub := p.(*packet.Unsubscribe)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(unsub.PacketID)).
		Int("Topics", len(unsub.Topics)).
		Uint8("Version", uint8(unsub.Version)).
		Msg("Received UNSUBSCRIBE packet")

	var s *Session
	s, err = h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(unsub.Version)).
			Msg("Failed to read session (UNSUBSCRIBE): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	codes, changed := h.unsubscribe(s, unsub)
	if changed {
		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("Failed to save session (UNSUBSCRIBE): " + err.Error())
			for i := 0; i < len(codes); i++ {
				codes[i] = packet.ReasonCodeV5UnspecifiedError
			}
		}
	}

	replies = make([]packet.Packet, 0, 1)

	unsubAck := packet.NewUnsubAck(unsub.PacketID, s.Version, codes, nil)
	replies = append(replies, &unsubAck)
	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint16("PacketId", uint16(unsubAck.PacketID)).
		Int("ReasonCodes", len(unsubAck.ReasonCodes)).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Uint8("Version", uint8(unsubAck.Version)).
		Msg("Sending UNSUBACK packet")
	return replies, nil
}

func (h *Unsubscribe) unsubscribe(s *Session, p *packet.Unsubscribe) (codes []packet.ReasonCode, changed bool) {
	codes = make([]packet.ReasonCode, 0, len(p.Topics))

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
					Msg("Failed to unsubscribe (UNSUBSCRIBE): " + msg)
			} else {
				code = packet.ReasonCodeV5NoSubscriptionExisted
				h.log.Warn().
					Str("ClientId", string(s.ClientID)).
					Uint64("SessionId", uint64(s.SessionID)).
					Str("TopicFilter", topic).
					Uint8("Version", uint8(s.Version)).
					Msg("Subscription not found (UNSUBSCRIBE)")
			}
			codes = append(codes, code)
			continue
		}

		delete(s.Subscriptions, topic)
		changed = true

		h.log.Info().
			Str("ClientId", string(s.ClientID)).
			Uint16("PacketId", uint16(p.PacketID)).
			Uint64("SessionId", uint64(s.SessionID)).
			Int("Subscriptions", len(s.Subscriptions)).
			Str("TopicFilter", topic).
			Int("Topics", len(p.Topics)).
			Uint8("Version", uint8(s.Version)).
			Msg("Client unsubscribed to topic")
		codes = append(codes, packet.ReasonCodeV5Success)
	}

	return codes, changed
}
