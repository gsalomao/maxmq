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

// PubAckHandler is responsible for handling the PUBACK packets.
type PubAckHandler struct {
	// Unexported fields
	log          *logger.Logger
	sessionStore SessionStore
}

// NewPubAckHandler creates a new NewPubAckHandler.
func NewPubAckHandler(st SessionStore, l *logger.Logger) *PubAckHandler {
	return &PubAckHandler{log: l, sessionStore: st}
}

// HandlePacket handles the given packet as PUBACK packet.
func (h *PubAckHandler) HandlePacket(id packet.ClientID, p packet.Packet) ([]packet.Packet, error) {
	pubAck := p.(*packet.PubAck)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubAck.PacketID)).
		Uint8("Version", uint8(pubAck.Version)).
		Msg("Received PUBACK packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubAck.Version)).
			Msg("Failed to read session (PUBACK): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	inflightMsg := s.findInflightMessage(pubAck.PacketID)
	if inflightMsg == nil {
		h.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.InflightMessages.Len()).
			Uint16("PacketId", uint16(pubAck.PacketID)).
			Uint8("Version", uint8(s.Version)).
			Msg("Received PUBACK with unknown packet ID")

		return nil, ErrPacketNotFound
	}
	s.InflightMessages.Remove(inflightMsg)

	err = h.sessionStore.SaveSession(s)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(s.ClientID)).
			Uint64("SessionId", uint64(s.SessionID)).
			Uint8("Version", uint8(s.Version)).
			Msg("Failed to save session (PUBACK): " + err.Error())
		return nil, err
	}

	msg := inflightMsg.Value.(*Message)
	h.log.Info().
		Str("ClientId", string(s.ClientID)).
		Int("InflightMessages", s.InflightMessages.Len()).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(msg.Packet.PacketID)).
		Uint8("QoS", uint8(msg.Packet.QoS)).
		Uint8("Retain", msg.Packet.Retain).
		Str("TopicName", msg.Packet.TopicName).
		Uint8("Version", uint8(msg.Packet.Version)).
		Msg("Message published to client (PUBACK)")

	return nil, nil
}
