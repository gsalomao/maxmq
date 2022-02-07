/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package packet

import (
	"errors"
	"fmt"
)

var (
	// The broker does not support the level of the MQTT protocol.
	ErrV3UnacceptableProtocolVersion = &Error{
		Code:   ReturnCodeV3UnacceptableProtocolVersion,
		Reason: "unacceptable protocol version",
	}

	// The client identifier is correct UTF-8 but not allowed.
	ErrV3IdentifierRejected = &Error{
		Code:   ReturnCodeV3IdentifierRejected,
		Reason: "client ID not allowed",
	}

	// Data within the CONNECT packet could not be correctly parsed.
	ErrV5MalformedPacket = &Error{
		Code:   ReturnCodeV5MalformedPacket,
		Reason: "malformed packet",
	}

	//  Data in the CONNECT packet does not conform with the V5 specification.
	ErrV5ProtocolError = &Error{
		Code:   ReturnCodeV5ProtocolError,
		Reason: "protocol error",
	}
)

// Error represents the errors related to the MQTT protocol.
type Error struct {
	// Code represents the error codes based on the MQTT specifications.
	Code ReturnCode

	// Reason is string with a human-friendly message about the error.
	Reason string
}

// Error returns a string with the error code and the reason of the error.
func (err Error) Error() string {
	return fmt.Sprintf("%d (%s)", err.Code, err.Reason)
}

func newErrMalformedPacket(v MQTTVersion, msg string) error {
	if v == MQTT_V5_0 {
		return ErrV5MalformedPacket
	}

	return errors.New(msg)
}
