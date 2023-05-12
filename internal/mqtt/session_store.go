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
	"sync"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

func newSessionStore(g IDGenerator, l *logger.Logger) *sessionStore {
	return &sessionStore{
		idGen:    g,
		log:      l.WithPrefix("mqtt.store"),
		sessions: make(map[packet.ClientID]*session),
	}
}

type sessionStore struct {
	log      *logger.Logger
	sessions map[packet.ClientID]*session
	idGen    IDGenerator
	mutex    sync.RWMutex
}

func (st *sessionStore) newSession(id packet.ClientID) *session {
	sID := st.idGen.NextID()
	s := &session{
		clientID:      id,
		sessionID:     sessionID(sID),
		subscriptions: make(map[string]*subscription),
		unAckMessages: make(map[packet.ID]*message),
	}

	st.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint64("SessionId", uint64(s.sessionID)).
		Msg("New session created")
	return s
}

func (st *sessionStore) readSession(id packet.ClientID) (s *session, err error) {
	st.log.Trace().Str("ClientId", string(id)).Msg("Reading session")

	var ok bool
	st.mutex.RLock()
	s, ok = st.sessions[id]
	st.mutex.RUnlock()

	if !ok {
		st.log.Debug().Str("ClientId", string(id)).Msg("Session not found")
		return nil, errSessionNotFound
	}

	s.restored = true
	st.log.Debug().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("Session read with success")
	return s, nil
}

func (st *sessionStore) saveSession(s *session) error {
	st.log.Trace().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("Saving session")

	st.mutex.Lock()
	st.sessions[s.clientID] = s
	st.mutex.Unlock()

	st.log.Debug().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("Session saved with success")
	return nil
}

func (st *sessionStore) deleteSession(s *session) error {
	st.log.Trace().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("Deleting session")

	st.mutex.Lock()
	defer st.mutex.Unlock()

	if _, ok := st.sessions[s.clientID]; !ok {
		st.log.Debug().
			Str("ClientId", string(s.clientID)).
			Uint64("SessionId", uint64(s.sessionID)).
			Uint8("Version", uint8(s.version)).
			Msg("Session not found")
		return errSessionNotFound
	}

	delete(st.sessions, s.clientID)
	st.log.Debug().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("Session deleted with success")
	return nil
}
