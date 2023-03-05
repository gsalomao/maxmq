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

// DisconnectHandler is responsible for handling DISCONNECT packets.
type DisconnectHandler struct {
	// Unexported fields
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
}

// NewDisconnectHandler creates a new DisconnectHandler.
func NewDisconnectHandler(
	st SessionStore, subMgr SubscriptionManager, l *logger.Logger,
) *DisconnectHandler {

	return &DisconnectHandler{
		log:             l,
		sessionStore:    st,
		subscriptionMgr: subMgr,
	}
}

// HandlePacket handles the given packet as a DISCONNECT packet.
func (h *DisconnectHandler) HandlePacket(
	id packet.ClientID, p packet.Packet,
) ([]packet.Packet, error) {
	discPkt := p.(*packet.Disconnect)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(discPkt.Version)).
		Msg("MQTT Received DISCONNECT packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(discPkt.Version)).
			Msg("MQTT Failed to read session (DISCONNECT): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if s.Version == packet.MQTT50 && discPkt.Properties != nil {
		var replies []packet.Packet

		replies, err = h.handleProperties(s, discPkt.Properties)
		if err != nil {
			return replies, err
		}
	}

	s.Connected = false
	if s.CleanSession && (s.Version != packet.MQTT50 || s.ExpiryInterval == 0) {
		h.unsubscribeAllTopics(s)
		err = h.sessionStore.DeleteSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to delete session (DISCONNECT)")
		}
	} else {
		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to save session (DISCONNECT)")
		}
	}

	h.log.Info().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Int("InflightMessages", s.InflightMessages.Len()).
		Uint32("SessionExpiryInterval", s.ExpiryInterval).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("MQTT Client disconnected")

	return nil, nil
}

func (h *DisconnectHandler) unsubscribeAllTopics(s *Session) {
	for _, sub := range s.Subscriptions {
		err := h.subscriptionMgr.Unsubscribe(s.ClientID, sub.TopicFilter)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Bool("Connected", s.Connected).
				Uint64("SessionId", uint64(s.SessionID)).
				Str("Topic", sub.TopicFilter).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to unsubscribe (DISCONNECT)")
		}
		delete(s.Subscriptions, sub.TopicFilter)
	}
}

func (h *DisconnectHandler) handleProperties(
	s *Session, props *packet.Properties,
) ([]packet.Packet, error) {
	interval := props.SessionExpiryInterval
	if interval != nil && *interval > 0 && s.ExpiryInterval == 0 {
		h.log.Debug().
			Str("ClientId", string(s.ClientID)).
			Uint8("Version", uint8(s.Version)).
			Uint32("SessionExpiryInterval", *interval).
			Uint64("SessionId", uint64(s.SessionID)).
			Msg("MQTT DISCONNECT with invalid Session Expiry Interval")

		replies := make([]packet.Packet, 0)
		discReply := packet.NewDisconnect(s.Version, packet.ReasonCodeV5ProtocolError, nil /*props*/)
		replies = append(replies, &discReply)
		return replies, packet.ErrV5ProtocolError
	}
	if interval != nil && *interval != s.ExpiryInterval {
		s.ExpiryInterval = *interval
	}

	return nil, nil
}
