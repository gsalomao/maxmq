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

func TestStoreReadSession(t *testing.T) {
	st := newStore()

	id := clientID('a')
	_, err := st.readSession(id)
	assert.Equal(t, errSessionNotFound, err)
}

func TestStoreSaveSession(t *testing.T) {
	st := newStore()

	id := clientID('a')
	s := &session{
		clientID:       id,
		connectedAt:    time.Now().Unix(),
		expiryInterval: 60,
	}
	st.saveSession(s)

	savedSession, err := st.readSession(id)
	require.Nil(t, err)
	assert.Equal(t, s, savedSession)
}

func TestStoreDeleteSession(t *testing.T) {
	st := newStore()

	id := clientID('a')
	s := session{
		clientID:       id,
		connectedAt:    time.Now().Unix(),
		expiryInterval: 60,
	}
	st.saveSession(&s)
	_, err := st.readSession(id)
	require.Nil(t, err)

	st.deleteSession(&s)
	_, err = st.readSession(id)
	assert.Equal(t, errSessionNotFound, err)
}
