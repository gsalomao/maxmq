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

package packet

import (
	"errors"
	"fmt"
)

var (
	// ErrV3UnacceptableProtocolVersion indicates that the broker does not
	// support the level of the MQTT protocol.
	ErrV3UnacceptableProtocolVersion = &Error{
		ReasonCode: ReasonCodeV3UnacceptableProtocolVersion,
		Reason:     "unacceptable protocol version",
	}

	// ErrV3IdentifierRejected indicates that the client identifier is correct
	// UTF-8 but not allowed.
	ErrV3IdentifierRejected = &Error{
		ReasonCode: ReasonCodeV3IdentifierRejected,
		Reason:     "client ID not allowed",
	}

	// ErrV5MalformedPacket indicates that data within the packet could not be
	// correctly parsed.
	ErrV5MalformedPacket = &Error{
		ReasonCode: ReasonCodeV5MalformedPacket,
		Reason:     "malformed packet",
	}

	// ErrV5ProtocolError indicates that data in the packet does not conform
	// with the V5.x specification.
	ErrV5ProtocolError = &Error{
		ReasonCode: ReasonCodeV5ProtocolError,
		Reason:     "protocol error",
	}

	// ErrV5InvalidClientID indicates that client ID in the packet is valid, but
	// it is not allowed by the broker.
	ErrV5InvalidClientID = &Error{
		ReasonCode: ReasonCodeV5InvalidClientID,
		Reason:     "invalid client identifier",
	}

	// ErrV5SubscriptionIDNotSupported indicates that the broker does not
	// support subscription identifiers.
	ErrV5SubscriptionIDNotSupported = &Error{
		ReasonCode: ReasonCodeV5SubscriptionIDNotSupported,
		Reason:     "subscription identifier not supported",
	}
)

// Error represents the errors related to the MQTT protocol.
type Error struct {
	// ReasonCode represents the error codes based on the MQTT specifications.
	ReasonCode ReasonCode

	// Reason is string with a human-friendly message about the error.
	Reason string
}

// Error returns a string with the error code and the reason of the error.
func (err Error) Error() string {
	return fmt.Sprintf("%s (code=%d)", err.Reason, err.ReasonCode)
}

func newErrMalformedPacket(v MQTTVersion, msg string) error {
	if v == MQTT50 {
		return ErrV5MalformedPacket
	}

	return errors.New(msg)
}
