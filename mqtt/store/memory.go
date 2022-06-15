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

// MemoryStore represents a store where data are saved in memory.
type MemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]*mqtt.Session
}

// NewMemoryStore creates a MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sessions: make(map[string]*mqtt.Session),
	}
}

// GetSession gets the session from in-memory session store.
func (s *MemoryStore) GetSession(id mqtt.ClientID) (*mqtt.Session,
	error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[string(id)]
	if !ok {
		return &mqtt.Session{}, mqtt.ErrSessionNotFound
	}

	return session, nil
}

// SaveSession saves the session into the in-memory session store.
func (s *MemoryStore) SaveSession(session *mqtt.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sessions[string(session.ClientID)] = session
	return nil
}

// DeleteSession deletes the session from the in-memory session store.
func (s *MemoryStore) DeleteSession(session *mqtt.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, string(session.ClientID))
	return nil
}
