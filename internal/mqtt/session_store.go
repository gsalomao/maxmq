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
	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

func newSessionStore(g IDGenerator, l *logger.Logger) *sessionStore {
	return &sessionStore{
		idGen:    g,
		log:      l.WithPrefix("store"),
		sessions: make(map[packet.ClientID]*handler.Session),
	}
}

type sessionStore struct {
	log      *logger.Logger
	sessions map[packet.ClientID]*handler.Session
	idGen    IDGenerator
	mutex    sync.RWMutex
}

// NewSession creates a new session for given client identifier.
func (st *sessionStore) NewSession(id packet.ClientID) *handler.Session {
	sID := st.idGen.NextID()
	s := &handler.Session{
		ClientID:      id,
		SessionID:     handler.SessionID(sID),
		Subscriptions: make(map[string]*handler.Subscription),
		UnAckMessages: make(map[packet.ID]*handler.Message),
	}

	st.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint64("SessionId", uint64(s.SessionID)).
		Msg("New session created")
	return s
}

// ReadSession reads the session for the given client identifier.
func (st *sessionStore) ReadSession(id packet.ClientID) (*handler.Session, error) {
	st.log.Trace().Str("ClientId", string(id)).Msg("Reading session")

	st.mutex.RLock()
	s, ok := st.sessions[id]
	st.mutex.RUnlock()

	if !ok {
		st.log.Debug().Str("ClientId", string(id)).Msg("Session not found")
		return nil, handler.ErrSessionNotFound
	}

	s.Restored = true
	st.log.Debug().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Bool("Connected", s.Connected).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Int("InflightMessages", s.InflightMessages.Len()).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("Session read with success")
	return s, nil
}

// SaveSession saves the given session.
func (st *sessionStore) SaveSession(s *handler.Session) error {
	st.log.Trace().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Bool("Connected", s.Connected).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Int("InflightMessages", s.InflightMessages.Len()).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("Saving session")

	st.mutex.Lock()
	st.sessions[s.ClientID] = s
	st.mutex.Unlock()

	st.log.Debug().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Bool("Connected", s.Connected).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Int("InflightMessages", s.InflightMessages.Len()).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("Session saved with success")
	return nil
}

// DeleteSession deletes the given Session.
func (st *sessionStore) DeleteSession(s *handler.Session) error {
	st.log.Trace().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Bool("Connected", s.Connected).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Int("InflightMessages", s.InflightMessages.Len()).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("Deleting session")

	st.mutex.Lock()
	defer st.mutex.Unlock()

	if _, ok := st.sessions[s.ClientID]; !ok {
		st.log.Debug().
			Str("ClientId", string(s.ClientID)).
			Uint64("SessionId", uint64(s.SessionID)).
			Uint8("Version", uint8(s.Version)).
			Msg("Session not found")
		return handler.ErrSessionNotFound
	}

	delete(st.sessions, s.ClientID)
	st.log.Debug().
		Bool("CleanSession", s.CleanSession).
		Str("ClientId", string(s.ClientID)).
		Bool("Connected", s.Connected).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Int("InflightMessages", s.InflightMessages.Len()).
		Int("KeepAlive", s.KeepAlive).
		Uint64("SessionId", uint64(s.SessionID)).
		Int("Subscriptions", len(s.Subscriptions)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(s.Version)).
		Msg("Session deleted with success")
	return nil
}
