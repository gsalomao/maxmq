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

// Store is responsible to store information relevant for the MQTT Listener.
type Store interface {
	// GetSession gets the Session from the store.
	GetSession(id ClientID) (Session, error)

	// SaveSession creates the Session into the store if it doesn't exist or
	// update the existing Session.
	SaveSession(s *Session) error

	// DeleteSession deletes the Session from the store.
	DeleteSession(s *Session) error
}
