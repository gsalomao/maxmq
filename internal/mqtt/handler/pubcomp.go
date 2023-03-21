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

// PubComp is responsible for handling PUBCOMP packets.
type PubComp struct {
	// Unexported fields
	log          *logger.Logger
	sessionStore SessionStore
}

// NewPubComp creates a new PubComp handler.
func NewPubComp(ss SessionStore, l *logger.Logger) *PubComp {
	return &PubComp{log: l, sessionStore: ss}
}

// HandlePacket handles the given packet as PUBCOMP packet.
func (h *PubComp) HandlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pubCompPkt := p.(*packet.PubComp)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubCompPkt.PacketID)).
		Uint8("Version", uint8(pubCompPkt.Version)).
		Msg("Received PUBCOMP packet")

	var s *Session
	s, err = h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubCompPkt.Version)).
			Msg("Failed to read session (PUBCOMP): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	inflightMsg := s.findInflightMessage(pubCompPkt.PacketID)
	if inflightMsg == nil {
		h.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.InflightMessages.Len()).
			Uint16("PacketId", uint16(pubCompPkt.PacketID)).
			Uint8("Version", uint8(s.Version)).
			Msg("Received PUBCOMP with unknown packet ID")
		return nil, ErrPacketNotFound
	}

	s.InflightMessages.Remove(inflightMsg)
	err = h.sessionStore.SaveSession(s)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(s.ClientID)).
			Uint64("SessionId", uint64(s.SessionID)).
			Uint8("Version", uint8(s.Version)).
			Msg("Failed to save session (PUBCOMP): " + err.Error())
		return nil, err
	}

	msg := inflightMsg.Value.(*Message)
	h.log.Info().
		Str("ClientId", string(s.ClientID)).
		Int("InflightMessages", s.InflightMessages.Len()).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(msg.PacketID)).
		Uint8("Version", uint8(s.Version)).
		Msg("Message published to client (PUBCOMP)")
	return nil, nil
}
