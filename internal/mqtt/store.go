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
	sessions map[clientID]*session
}

func newStore() store {
	return store{
		sessions: make(map[clientID]*session),
	}
}

func (st *store) readSession(id clientID) (*session, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()

	s, ok := st.sessions[id]
	if !ok {
		return nil, errSessionNotFound
	}

	return s, nil
}

func (st *store) saveSession(s *session) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.sessions[s.clientID] = s
}

func (st *store) deleteSession(s *session) {
	st.mu.Lock()
	defer st.mu.Unlock()

	delete(st.sessions, s.clientID)
}
