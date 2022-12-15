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

// ReasonCode is a one byte unsigned value that indicates the result of an
// operation, based on the MQTT specifications.
type ReasonCode byte

const (
	// ReasonCodeV3ConnectionAccepted indicates that connection was accepted.
	ReasonCodeV3ConnectionAccepted ReasonCode = 0x00

	// ReasonCodeV3GrantedQoS0 indicates that the subscription was accepted and
	// the maximum QoS sent will be QoS 0.
	ReasonCodeV3GrantedQoS0 ReasonCode = 0x00

	// ReasonCodeV3GrantedQoS1 indicates that the subscription was accepted and
	// the maximum QoS sent will be QoS 1.
	ReasonCodeV3GrantedQoS1 ReasonCode = 0x01

	// ReasonCodeV3GrantedQoS2 indicates that the subscription was accepted and
	// any received QoS will be sent.
	ReasonCodeV3GrantedQoS2 ReasonCode = 0x02

	// ReasonCodeV3UnacceptableProtocolVersion indicates that the broker does
	// not support the level of the MQTT protocol.
	ReasonCodeV3UnacceptableProtocolVersion ReasonCode = 0x01

	// ReasonCodeV3IdentifierRejected indicates that the client identifier is
	// correct UTF-8 but not allowed.
	ReasonCodeV3IdentifierRejected ReasonCode = 0x02

	// ReasonCodeV3ServerUnavailable indicates that the MQTT service is
	// unavailable.
	ReasonCodeV3ServerUnavailable ReasonCode = 0x03

	// ReasonCodeV3BadUsernamePassword indicates that the data in the UserName
	// or Password is malformed.
	ReasonCodeV3BadUsernamePassword ReasonCode = 0x04

	// ReasonCodeV3NotAuthorized indicates that the client is not authorized to
	// connect.
	ReasonCodeV3NotAuthorized ReasonCode = 0x05

	// ReasonCodeV3Failure indicates that an error happened.
	ReasonCodeV3Failure ReasonCode = 0x80
)

const (
	// ReasonCodeV5Success indicates success.
	ReasonCodeV5Success ReasonCode = 0x00

	// ReasonCodeV5NormalDisconnection indicates to close the connection
	// normally and do not send the Will Message.
	ReasonCodeV5NormalDisconnection ReasonCode = 0x00

	// ReasonCodeV5GrantedQoS0 indicates that the subscription was accepted and
	// the maximum QoS sent will be QoS 0.
	ReasonCodeV5GrantedQoS0 ReasonCode = 0x00

	// ReasonCodeV5GrantedQoS1 indicates that the subscription was accepted and
	// the maximum QoS sent will be QoS 1.
	ReasonCodeV5GrantedQoS1 ReasonCode = 0x01

	// ReasonCodeV5GrantedQoS2 indicates that the subscription was accepted and
	// any received QoS will be sent.
	ReasonCodeV5GrantedQoS2 ReasonCode = 0x02

	// ReasonCodeV5DisconnectWithWillMessage indicates to close the connection,
	// but the broker shall send the Will Message.
	ReasonCodeV5DisconnectWithWillMessage ReasonCode = 0x04

	// ReasonCodeV5NoMatchingSubscribers indicates that the message was accepted
	// but there are no subscribers.
	ReasonCodeV5NoMatchingSubscribers ReasonCode = 0x10

	// ReasonCodeV5NoSubscriptionExisted indicates that no matching Topic Filter
	// is being used.
	ReasonCodeV5NoSubscriptionExisted ReasonCode = 0x11

	// ReasonCodeV5ContinueAuthentication indicates to continue the
	// authentication with another step.
	ReasonCodeV5ContinueAuthentication ReasonCode = 0x18

	// ReasonCodeV5ReAuthenticate indicates to initiate a re-authentication.
	ReasonCodeV5ReAuthenticate ReasonCode = 0x19

	// ReasonCodeV5UnspecifiedError indicates that the broker does not wish to
	// reveal the reason for the failure, or none of the other codes apply.
	ReasonCodeV5UnspecifiedError ReasonCode = 0x80

	// ReasonCodeV5MalformedPacket indicates that data within the CONNECT packet
	// could not be correctly parsed.
	ReasonCodeV5MalformedPacket ReasonCode = 0x81

	// ReasonCodeV5ProtocolError indicates that data in the packet does not
	// conform with the V5.x specification.
	ReasonCodeV5ProtocolError ReasonCode = 0x82

	// ReasonCodeV5ImplementationError indicates that the packet is valid but
	// is not accepted by the broker.
	ReasonCodeV5ImplementationError ReasonCode = 0x83

	// ReasonCodeV5UnsupportedProtocolVersion indicates that the broker does not
	// support the version of the MQTT protocol requested by the client.
	ReasonCodeV5UnsupportedProtocolVersion ReasonCode = 0x84

	// ReasonCodeV5InvalidClientID indicates that the Client ID is a valid
	// string but is not allowed by the broker.
	ReasonCodeV5InvalidClientID ReasonCode = 0x85

	// ReasonCodeV5BadUserNameOrPassword indicates that the broker does not
	// accept the UserName or Password specified by the client.
	ReasonCodeV5BadUserNameOrPassword ReasonCode = 0x86

	// ReasonCodeV5NotAuthorized indicates the client is not authorized to
	// perform the action.
	ReasonCodeV5NotAuthorized ReasonCode = 0x87

	// ReasonCodeV5ServerUnavailable indicates that the MQTT broker is not
	// available.
	ReasonCodeV5ServerUnavailable ReasonCode = 0x88

	// ReasonCodeV5ServerBusy indicates that the broker is busy. Try again
	// later.
	ReasonCodeV5ServerBusy ReasonCode = 0x89

	// ReasonCodeV5Banned indicates that the client has been banned by
	// administrative action.
	ReasonCodeV5Banned ReasonCode = 0x8A

	// ReasonCodeV5ServerShuttingDown indicates that the broker is shutting
	// down.
	ReasonCodeV5ServerShuttingDown ReasonCode = 0x8B

	// ReasonCodeV5BadAuthMethod indicates that the authentication method is not
	// supported or does not match the authentication method currently in use.
	ReasonCodeV5BadAuthMethod ReasonCode = 0x8C

	// ReasonCodeV5KeepAliveTimeout indicates that connection is closed because
	// no packet has been received for 1.5 times the Keepalive time.
	ReasonCodeV5KeepAliveTimeout ReasonCode = 0x8D

	// ReasonCodeV5SessionTakeOver indicates that another connection using the
	// same client identified has connected.
	ReasonCodeV5SessionTakeOver ReasonCode = 0x8E

	// ReasonCodeV5TopicFilterInvalid indicates that the topic filter is
	// correctly formed but is not allowed.
	ReasonCodeV5TopicFilterInvalid ReasonCode = 0x8F

	// ReasonCodeV5TopicNameInvalid indicates that the topic name is correctly
	// formed, but is not accepted.
	ReasonCodeV5TopicNameInvalid ReasonCode = 0x90

	// ReasonCodeV5PacketIDInUse indicates that the specified packet ID is
	// already in use.
	ReasonCodeV5PacketIDInUse ReasonCode = 0x91

	// ReasonCodeV5PacketIDNotFound indicates that the packet ID was not found.
	ReasonCodeV5PacketIDNotFound ReasonCode = 0x92

	// ReasonCodeV5ReceiveMaximumExceeded indicates that has been received more
	// than Receive Maximum publication for which it has not sent PUBACK or
	// PUBCOMP.
	ReasonCodeV5ReceiveMaximumExceeded ReasonCode = 0x93

	// ReasonCodeV5TopicAliasInvalid indicates that has received a PUBLISH
	// Packet containing a topic alias which is greater than the maximum topic
	// alias sent in the CONNECT or CONNACK packet.
	ReasonCodeV5TopicAliasInvalid ReasonCode = 0x94

	// ReasonCodeV5PacketTooLarge indicates that the packet size is greater than
	// maximum packet size.
	ReasonCodeV5PacketTooLarge ReasonCode = 0x95

	// ReasonCodeV5MessageRateTooHigh indicates that the received data rate is
	// too high.
	ReasonCodeV5MessageRateTooHigh ReasonCode = 0x96

	// ReasonCodeV5QuotaExceeded indicates that an implementation or
	// administrative imposed limit has been exceeded.
	ReasonCodeV5QuotaExceeded ReasonCode = 0x97

	// ReasonCodeV5AdministrativeAction indicates that the connection is closed
	// by administrative action.
	ReasonCodeV5AdministrativeAction ReasonCode = 0x98

	// ReasonCodeV5PayloadFormatInvalid indicates that the payload format does
	// not match the specified payload format indicator.
	ReasonCodeV5PayloadFormatInvalid ReasonCode = 0x99

	// ReasonCodeV5RetainNotSupported indicates that the broker does not support
	// retained messages.
	ReasonCodeV5RetainNotSupported ReasonCode = 0x9A

	// ReasonCodeV5QoSNotSupported indicates that the client specified a QoS
	// greater than the QoS specified in a Maximum QoS in the CONNACK.
	ReasonCodeV5QoSNotSupported ReasonCode = 0x9B

	// ReasonCodeV5UseAnotherServer indicates that the client should temporarily
	// use another server.
	ReasonCodeV5UseAnotherServer ReasonCode = 0x9C

	// ReasonCodeV5ServerMoved indicates that the client should permanently use
	// another server.
	ReasonCodeV5ServerMoved ReasonCode = 0x9D

	// ReasonCodeV5SharedSubscriptionsNotSupported indicates that the broker
	// does not support Shared Subscriptions.
	ReasonCodeV5SharedSubscriptionsNotSupported ReasonCode = 0x9E

	// ReasonCodeV5ConnectionRateExceeded indicates that the connection rate
	// limit has been exceeded.
	ReasonCodeV5ConnectionRateExceeded ReasonCode = 0x9F

	// ReasonCodeV5MaximumConnectTime indicates that the maximum connection time
	// authorized for this connection has been exceeded.
	ReasonCodeV5MaximumConnectTime ReasonCode = 0xA0

	// ReasonCodeV5SubscriptionIDNotSupported indicates that the broker does not
	// support Subscription Identifiers.
	ReasonCodeV5SubscriptionIDNotSupported ReasonCode = 0xA1

	// ReasonCodeV5WildcardSubscriptionsNotSupported indicates that the broker
	// does not support Wildcard Subscriptions.
	ReasonCodeV5WildcardSubscriptionsNotSupported ReasonCode = 0xA2
)
