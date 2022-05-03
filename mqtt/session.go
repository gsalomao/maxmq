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

import "errors"

// ErrSessionNotFound indicates that the session was not found in the store.
var ErrSessionNotFound = errors.New("session not found")

// Session stores the MQTT session.
type Session struct {
	// ConnectedAt represents the timestamp which the session was created.
	ConnectedAt int64

	// ExpiryInterval represents the interval, in seconds, which the session
	// expires.
	ExpiryInterval uint32
}

// SessionStore is responsible for manage sessions in the store.
type SessionStore interface {
	// GetSession gets the session from the store.
	GetSession(id ClientID) (Session, error)

	// SaveSession saves the session into the store.
	SaveSession(id ClientID, s Session) error
}
