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

// ReturnCode represents the return code based on the MQTT specifications.
type ReturnCode byte

const (
	// Connection accepted.
	ReturnCodeV3ConnectionAccepted ReturnCode = 0x00

	// The broker does not support the level of the MQTT protocol.
	ReturnCodeV3UnacceptableProtocolVersion ReturnCode = 0x01

	// The client identifier is correct UTF-8 but not allowed.
	ReturnCodeV3IdentifierRejected ReturnCode = 0x02

	// The MQTT service is unavailable.
	ReturnCodeV3ServerUnavailable ReturnCode = 0x03

	// The data in the user name or password is malformed.
	ReturnCodeV3BadUsernamePassword ReturnCode = 0x04

	// The client is not authorized to connect.
	ReturnCodeV3NotAuthorized ReturnCode = 0x05
)

const (
	// The Connection is accepted.
	ReturnCodeV5Success ReturnCode = 0x00

	// The broker does not wish to reveal the reason for the failure, or none
	// of the other codes apply.
	ReturnCodeV5UnspecifiedError ReturnCode = 0x80

	// Data within the CONNECT packet could not be correctly parsed.
	ReturnCodeV5MalformedPacket ReturnCode = 0x81

	// Data in the CONNECT packet does not conform with the V5 specification.
	ReturnCodeV5ProtocolError ReturnCode = 0x82

	// The CONNECT is valid but is not accepted by this broker.
	ReturnCodeV5ImplementationError ReturnCode = 0x83

	// The broker does not support the version of the MQTT protocol requested
	// by the client.
	ReturnCodeV5UnsupportedProtocolVersion ReturnCode = 0x84

	// The Client ID is a valid string but is not allowed by the broker.
	ReturnCodeV5InvalidClientID ReturnCode = 0x85

	// The broker does not accept the User Name or Password specified by the
	// client.
	ReturnCodeV5BadUsernamePassword ReturnCode = 0x86

	// The client is not authorized to connect.
	ReturnCodeV5NotAuthorized ReturnCode = 0x87

	// The MQTT broker is not available.
	ReturnCodeV5ServerUnavailable ReturnCode = 0x88

	// The broker is busy. Try again later.
	ReturnCodeV5ServerBusy ReturnCode = 0x89

	// The client has been banned by administrative action.
	ReturnCodeV5Banned ReturnCode = 0x8A

	// The authentication method is not supported or does not match the
	// authentication method currently in use.
	ReturnCodeV5BadAuthMethod ReturnCode = 0x8C

	// The Will Topic Name is not malformed, but is not accepted by the broker.
	ReturnCodeV5InvalidTopicName ReturnCode = 0x90

	// The CONNECT packet exceeded the maximum permissible size.
	ReturnCodeV5PacketTooLarge ReturnCode = 0x95

	// An implementation or administrative imposed limit has been exceeded.
	ReturnCodeV5QuotaExceeded ReturnCode = 0x97

	// The Will Payload does not match the specified Payload Format Indicator.
	ReturnCodeV5InvalidPayloadFormat ReturnCode = 0x99

	// The broker does not support retained messages, and Will Retain was set
	// to 1.
	ReturnCodeV5UnsupportedRetain ReturnCode = 0x9A

	// The broker does not support the QoS set in Will QoS.
	ReturnCodeV5UnsupportedQoS ReturnCode = 0x9B

	// The client should temporarily use another server.
	ReturnCodeV5UseAnotherServer ReturnCode = 0x9C

	// The client should permanently use another server.
	ReturnCodeV5ServerMoved ReturnCode = 0x9D

	// The connection rate limit has been exceeded.
	ReturnCodeV5ConnectionRateExceeded ReturnCode = 0x9F
)
