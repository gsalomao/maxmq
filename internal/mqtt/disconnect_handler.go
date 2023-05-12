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

type disconnectHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
	pubSub       *pubSub
}

func newDisconnectHandler(ss *sessionStore, ps *pubSub, l *logger.Logger) *disconnectHandler {
	return &disconnectHandler{
		log:          l.WithPrefix("mqtt.disconnect"),
		sessionStore: ss,
		pubSub:       ps,
	}
}

func (h *disconnectHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	discPkt := p.(*packet.Disconnect)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(discPkt.Version)).
		Msg("Received DISCONNECT packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(discPkt.Version)).
			Msg("Failed to read session (DISCONNECT): " + err.Error())
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.version == packet.MQTT50 && discPkt.Properties != nil {
		replies, err = h.handleProperties(s, discPkt.Properties)
		if err != nil {
			return replies, err
		}
	}

	s.connected = false
	if s.cleanSession && (s.version != packet.MQTT50 || s.expiryInterval == 0) {
		h.unsubscribeAllTopics(s)
		err = h.sessionStore.deleteSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to delete session (DISCONNECT)")
		}
	} else {
		err = h.sessionStore.saveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to save session (DISCONNECT)")
		}
	}

	h.log.Info().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint32("SessionExpiryInterval", s.expiryInterval).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("Client disconnected")
	return nil, nil
}

func (h *disconnectHandler) unsubscribeAllTopics(s *session) {
	for _, sub := range s.subscriptions {
		err := h.pubSub.unsubscribe(s.clientID, sub.topicFilter)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Bool("Connected", s.connected).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("Topic", sub.topicFilter).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to unsubscribe (DISCONNECT)")
		}
		delete(s.subscriptions, sub.topicFilter)
	}
}

func (h *disconnectHandler) handleProperties(s *session, p *packet.Properties) (replies []packet.Packet, err error) {
	interval := p.SessionExpiryInterval
	if interval != nil && *interval > 0 && s.expiryInterval == 0 {
		h.log.Debug().
			Str("ClientId", string(s.clientID)).
			Uint8("Version", uint8(s.version)).
			Uint32("SessionExpiryInterval", *interval).
			Uint64("SessionId", uint64(s.sessionID)).
			Msg("Packet DISCONNECT with invalid Session Expiry Interval")

		replies = make([]packet.Packet, 0)
		discReply := packet.NewDisconnect(s.version, packet.ReasonCodeV5ProtocolError, nil)
		replies = append(replies, &discReply)
		return replies, packet.ErrV5ProtocolError
	}
	if interval != nil && *interval != s.expiryInterval {
		s.expiryInterval = *interval
	}

	return nil, nil
}
