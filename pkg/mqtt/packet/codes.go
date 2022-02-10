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
	// ReturnCodeV3ConnectionAccepted indicates that connection was accepted.
	ReturnCodeV3ConnectionAccepted ReturnCode = 0x00

	// ReturnCodeV3UnacceptableProtocolVersion indicates that the broker does
	// not support the level of the MQTT protocol.
	ReturnCodeV3UnacceptableProtocolVersion ReturnCode = 0x01

	// ReturnCodeV3IdentifierRejected indicates that the client identifier is
	// correct UTF-8 but not allowed.
	ReturnCodeV3IdentifierRejected ReturnCode = 0x02

	// ReturnCodeV3ServerUnavailable indicates that the MQTT service is
	// unavailable.
	ReturnCodeV3ServerUnavailable ReturnCode = 0x03

	// ReturnCodeV3BadUsernamePassword indicates that the data in the UserName
	// or Password is malformed.
	ReturnCodeV3BadUsernamePassword ReturnCode = 0x04

	// ReturnCodeV3NotAuthorized indicates that the client is not authorized to
	// connect.
	ReturnCodeV3NotAuthorized ReturnCode = 0x05
)

const (
	// ReturnCodeV5Success indicates that the connection was accepted.
	ReturnCodeV5Success ReturnCode = 0x00

	// ReturnCodeV5UnspecifiedError indicates that the broker does not wish to
	// reveal the reason for the failure, or none of the other codes apply.
	ReturnCodeV5UnspecifiedError ReturnCode = 0x80

	// ReturnCodeV5MalformedPacket indicates that data within the CONNECT packet
	// could not be correctly parsed.
	ReturnCodeV5MalformedPacket ReturnCode = 0x81

	// ReturnCodeV5ProtocolError indicates that data in the CONNECT packet does
	// not conform with the V5.x specification.
	ReturnCodeV5ProtocolError ReturnCode = 0x82

	// ReturnCodeV5ImplementationError indicates that the CONNECT is valid but
	// is not accepted by the broker.
	ReturnCodeV5ImplementationError ReturnCode = 0x83

	// ReturnCodeV5UnsupportedProtocolVersion indicates that the broker does not
	// support the version of the MQTT protocol requested by the client.
	ReturnCodeV5UnsupportedProtocolVersion ReturnCode = 0x84

	// ReturnCodeV5InvalidClientID indicates that the Client ID is a valid
	// string but is not allowed by the broker.
	ReturnCodeV5InvalidClientID ReturnCode = 0x85

	// ReturnCodeV5BadUserNameOrPassword indicates that the broker does not
	// accept the UserName or Password specified by the client.
	ReturnCodeV5BadUserNameOrPassword ReturnCode = 0x86

	// ReturnCodeV5NotAuthorized indicates the client is not authorized to
	// connect.
	ReturnCodeV5NotAuthorized ReturnCode = 0x87

	// ReturnCodeV5ServerUnavailable indicates that the MQTT broker is not
	// available.
	ReturnCodeV5ServerUnavailable ReturnCode = 0x88

	// ReturnCodeV5ServerBusy indicates that the broker is busy. Try again
	// later.
	ReturnCodeV5ServerBusy ReturnCode = 0x89

	// ReturnCodeV5Banned indicates that the client has been banned by
	// administrative action.
	ReturnCodeV5Banned ReturnCode = 0x8A

	// ReturnCodeV5BadAuthMethod indicates that the authentication method is not
	// supported or does not match the authentication method currently in use.
	ReturnCodeV5BadAuthMethod ReturnCode = 0x8C

	// ReturnCodeV5InvalidTopicName indicates that the Will Topic Name is not
	// malformed, but is not accepted by the broker.
	ReturnCodeV5InvalidTopicName ReturnCode = 0x90

	// ReturnCodeV5PacketTooLarge indicates that the CONNECT packet exceeded the
	// maximum permissible size.
	ReturnCodeV5PacketTooLarge ReturnCode = 0x95

	// ReturnCodeV5QuotaExceeded indicates that an implementation or
	// administrative imposed limit has been exceeded.
	ReturnCodeV5QuotaExceeded ReturnCode = 0x97

	// ReturnCodeV5InvalidPayloadFormat indicates that the Will Payload does not
	// match the specified Payload Format Indicator.
	ReturnCodeV5InvalidPayloadFormat ReturnCode = 0x99

	// ReturnCodeV5UnsupportedRetain indicates that the broker does not support
	// retained messages, and Will Retain was set to 1.
	ReturnCodeV5UnsupportedRetain ReturnCode = 0x9A

	// ReturnCodeV5UnsupportedQoS indicates that the broker does not support the
	// QoS set in Will QoS.
	ReturnCodeV5UnsupportedQoS ReturnCode = 0x9B

	// ReturnCodeV5UseAnotherServer indicates that the client should temporarily
	// use another server.
	ReturnCodeV5UseAnotherServer ReturnCode = 0x9C

	// ReturnCodeV5ServerMoved indicates that the client should permanently use
	// another server.
	ReturnCodeV5ServerMoved ReturnCode = 0x9D

	// ReturnCodeV5ConnectionRateExceeded indicates that the connection rate
	// limit has been exceeded.
	ReturnCodeV5ConnectionRateExceeded ReturnCode = 0x9F
)
