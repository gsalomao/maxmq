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
		Code:   ErrCodeV3UnacceptableProtocolVersion,
		Reason: "unacceptable protocol version",
	}

	// The client identifier is correct UTF-8 but not allowed.
	ErrV3IdentifierRejected = &Error{
		Code:   ErrCodeV3IdentifierRejected,
		Reason: "client ID not allowed",
	}

	// Data within the CONNECT packet could not be correctly parsed.
	ErrV5MalformedPacket = &Error{
		Code:   ErrCodeV5MalformedPacket,
		Reason: "malformed packet",
	}

	//  Data in the CONNECT packet does not conform with the V5 specification.
	ErrV5ProtocolError = &Error{
		Code:   ErrCodeV5ProtocolError,
		Reason: "protocol error",
	}
)

// ErrCode represents the error code based on the MQTT specifications.
type ErrCode byte

const (
	// The broker does not support the level of the MQTT protocol.
	ErrCodeV3UnacceptableProtocolVersion ErrCode = 0x01

	// The client identifier is correct UTF-8 but not allowed.
	ErrCodeV3IdentifierRejected ErrCode = 0x02

	// The MQTT service is unavailable.
	ErrCodeV3ServerUnavailable ErrCode = 0x03

	// The data in the user name or password is malformed.
	ErrCodeV3BadUsernamePassword ErrCode = 0x04

	// The client is not authorized to connect.
	ErrCodeV3NotAuthorized ErrCode = 0x05
)

const (
	// The broker does not wish to reveal the reason for the failure, or none
	// of the other codes apply.
	ErrCodeV5UnspecifiedError ErrCode = 0x80

	// Data within the CONNECT packet could not be correctly parsed.
	ErrCodeV5MalformedPacket ErrCode = 0x81

	// Data in the CONNECT packet does not conform with the V5 specification.
	ErrCodeV5ProtocolError ErrCode = 0x82

	// The CONNECT is valid but is not accepted by this broker.
	ErrCodeV5ImplementationError ErrCode = 0x83

	// The broker does not support the version of the MQTT protocol requested
	// by the client.
	ErrCodeV5UnsupportedProtocolVersion ErrCode = 0x84

	// The Client ID is a valid string but is not allowed by the broker.
	ErrCodeV5InvalidClientID ErrCode = 0x85

	// The broker does not accept the User Name or Password specified by the
	// client.
	ErrCodeV5BadUsernamePassword ErrCode = 0x86

	// The client is not authorized to connect.
	ErrCodeV5NotAuthorized ErrCode = 0x87

	// The MQTT broker is not available.
	ErrCodeV5ServerUnavailable ErrCode = 0x88

	// The broker is busy. Try again later.
	ErrCodeV5ServerBusy ErrCode = 0x89

	// The client has been banned by administrative action.
	ErrCodeV5Banned ErrCode = 0x8A

	// The authentication method is not supported or does not match the
	// authentication method currently in use.
	ErrCodeV5BadAuthMethod ErrCode = 0x8C

	// The Will Topic Name is not malformed, but is not accepted by the broker.
	ErrCodeV5InvalidTopicName ErrCode = 0x90

	// The CONNECT packet exceeded the maximum permissible size.
	ErrCodeV5PacketTooLarge ErrCode = 0x95

	// An implementation or administrative imposed limit has been exceeded.
	ErrCodeV5QuotaExceeded ErrCode = 0x97

	// The Will Payload does not match the specified Payload Format Indicator.
	ErrCodeV5InvalidPayloadFormat ErrCode = 0x99

	// The broker does not support retained messages, and Will Retain was set
	// to 1.
	ErrCodeV5UnsupportedRetain ErrCode = 0x9A

	// The broker does not support the QoS set in Will QoS.
	ErrCodeV5UnsupportedQoS ErrCode = 0x9B

	// The client should temporarily use another server.
	ErrCodeV5UseAnotherServer ErrCode = 0x9C

	// The client should permanently use another server.
	ErrCodeV5ServerMoved ErrCode = 0x9D

	// The connection rate limit has been exceeded.
	ErrCodeV5ConnectionRateExceeded ErrCode = 0x9F
)

// Error represents the errors related to the MQTT protocol.
type Error struct {
	// Code represents the error codes based on the MQTT specifications.
	Code ErrCode

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
