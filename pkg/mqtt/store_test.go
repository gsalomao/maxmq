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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_ReadSession(t *testing.T) {
	s := newStore()

	id := ClientID{'a'}
	_, err := s.readSession(id)
	assert.Equal(t, errSessionNotFound, err)
}

func TestStore_SaveSession(t *testing.T) {
	s := newStore()

	id := ClientID{'a'}
	session := &Session{
		ClientID:       id,
		ConnectedAt:    time.Now().Unix(),
		ExpiryInterval: 60,
	}
	s.saveSession(session)

	savedSession, err := s.readSession(id)
	require.Nil(t, err)
	assert.Equal(t, session, savedSession)
}

func TestStore_DeleteSession(t *testing.T) {
	s := newStore()

	id := ClientID{'a'}
	session := Session{
		ClientID:       id,
		ConnectedAt:    time.Now().Unix(),
		ExpiryInterval: 60,
	}
	s.saveSession(&session)
	_, err := s.readSession(id)
	require.Nil(t, err)

	s.deleteSession(&session)
	_, err = s.readSession(id)
	assert.Equal(t, errSessionNotFound, err)
}
