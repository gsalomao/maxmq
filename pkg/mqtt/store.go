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

package mqtt

import (
	"sync"
)

type store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func newStore() store {
	return store{
		sessions: make(map[string]*Session),
	}
}

func (s *store) readSession(id ClientID) (*Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[string(id)]
	if !ok {
		return nil, errSessionNotFound
	}

	return session, nil
}

func (s *store) saveSession(session *Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sessions[string(session.ClientID)] = session
}

func (s *store) deleteSession(session *Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, string(session.ClientID))
}
