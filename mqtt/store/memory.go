// Copyright 2022 The MaxMQ Authors
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

package store

import (
	"sync"

	"github.com/gsalomao/maxmq/mqtt"
)

// MemorySessionStore represents a store where the sessions are saved in
// memory.
type MemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]mqtt.Session
}

// NewMemorySessionStore creates a MemorySessionStore.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		sessions: make(map[string]mqtt.Session),
	}
}

// GetSession gets the session from in-memory session store.
func (s *MemorySessionStore) GetSession(id mqtt.ClientID) (mqtt.Session,
	error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[string(id)]
	if !ok {
		return mqtt.Session{}, mqtt.ErrSessionNotFound
	}

	return session, nil
}

// SaveSession saves the session into the in-memory session store.
func (s *MemorySessionStore) SaveSession(id mqtt.ClientID,
	session mqtt.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sessions[string(id)] = session
	return nil
}

// DeleteSession deletes the session from the in-memory session store.
func (s *MemorySessionStore) DeleteSession(id mqtt.ClientID,
	_ mqtt.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, string(id))
	return nil
}
