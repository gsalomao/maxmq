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

// PubRecHandler is responsible for handling PUBREC packets.
type PubRecHandler struct {
	// Unexported fields
	log          *logger.Logger
	sessionStore SessionStore
}

// NewPubRecHandler creates a new NewPubRecHandler.
func NewPubRecHandler(st SessionStore, l *logger.Logger) *PubRecHandler {
	return &PubRecHandler{
		log:          l,
		sessionStore: st,
	}
}

// HandlePacket handles the given packet as a PUBREC packet.
func (h *PubRecHandler) HandlePacket(id packet.ClientID,
	pkt packet.Packet) ([]packet.Packet, error) {

	pubRecPkt := pkt.(*packet.PubRec)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubRecPkt.PacketID)).
		Uint8("Version", uint8(pubRecPkt.Version)).
		Msg("MQTT Received PUBREC packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubRecPkt.Version)).
			Msg("MQTT Failed to read session (PUBREC): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	replies := make([]packet.Packet, 0, 1)
	inflightMsg := s.findInflightMessage(pubRecPkt.PacketID)
	if inflightMsg == nil {
		h.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.InflightMessages.Len()).
			Uint16("PacketId", uint16(pubRecPkt.PacketID)).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Received PUBREC with unknown packet ID")

		if s.Version != packet.MQTT50 {
			return nil, ErrPacketNotFound
		}

		pubRelPkt := packet.NewPubRel(pubRecPkt.PacketID, s.Version,
			packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubRelPkt)

		return replies, nil
	}

	msg := inflightMsg.Value.(*Message)
	h.log.Debug().
		Str("ClientId", string(s.ClientID)).
		Int("InflightMessages", s.InflightMessages.Len()).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(msg.PacketID)).
		Uint8("Version", uint8(s.Version)).
		Msg("MQTT Client received the packet (PUBREC)")

	if msg.Packet != nil {
		msg.Packet = nil
		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to save session (PUBREC): " + err.Error())
			return nil, err
		}
	}

	pubRelPkt := packet.NewPubRel(pubRecPkt.PacketID, s.Version,
		packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubRelPkt)

	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(pubRelPkt.PacketID)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(pubRelPkt.Version)).
		Msg("MQTT Sending PUBREL packet")

	return replies, nil
}
