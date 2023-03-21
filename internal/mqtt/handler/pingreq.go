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

// PingReq is responsible for handling PINGREQ packets.
type PingReq struct {
	// Unexported fields
	log          *logger.Logger
	sessionStore SessionStore
}

// NewPingReq creates a new PingReq handler.
func NewPingReq(ss SessionStore, l *logger.Logger) *PingReq {
	return &PingReq{log: l, sessionStore: ss}
}

// HandlePacket handles the given packet as PINGREQ packet.
func (h *PingReq) HandlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pingReq := p.(*packet.PingReq)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(pingReq.Version)).
		Msg("Received PINGREQ packet")

	var s *Session
	s, err = h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pingReq.Version)).
			Msg("Failed to read session (PINGREQ): " + err.Error())
		return nil, err
	}

	s.Mutex.RLock()
	defer s.Mutex.RUnlock()

	replies = make([]packet.Packet, 0, 1)
	pingResp := &packet.PingResp{}
	replies = append(replies, pingResp)
	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Uint8("Version", uint8(s.Version)).
		Msg("Sending PINGRESP packet")
	return replies, nil
}
