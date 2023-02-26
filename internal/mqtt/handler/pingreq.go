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

// PingReqHandler is responsible for handling PINGREQ packets.
type PingReqHandler struct {
	// Unexported fields
	log          *logger.Logger
	sessionStore SessionStore
}

// NewPingReqHandler creates a new NewPingReqHandler.
func NewPingReqHandler(st SessionStore, l *logger.Logger) *PingReqHandler {
	return &PingReqHandler{
		log:          l,
		sessionStore: st,
	}
}

// HandlePacket handles the given packet as a PINGREQ packet.
func (h *PingReqHandler) HandlePacket(id packet.ClientID,
	pkt packet.Packet) ([]packet.Packet, error) {

	pingReqPkt := pkt.(*packet.PingReq)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(pingReqPkt.Version)).
		Msg("MQTT Received PINGREQ packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pingReqPkt.Version)).
			Msg("MQTT Failed to read session (PINGREQ): " + err.Error())
		return nil, err
	}

	s.Mutex.RLock()
	defer s.Mutex.RUnlock()

	pingRespPkt := packet.PingResp{}
	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Uint8("Version", uint8(s.Version)).
		Msg("MQTT Sending PINGRESP packet")

	return []packet.Packet{&pingRespPkt}, nil
}
