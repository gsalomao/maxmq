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
	"bytes"
)

// Properties contains all properties available in the MQTT specification
// version 5.0.
type Properties struct {
	// PayloadFormatIndicator indicates whether the Will Message is a UTF-8
	// string or not.
	PayloadFormatIndicator *byte

	// MessageExpiryInterval represents the lifetime, in seconds, of the Will
	// Message.
	MessageExpiryInterval *uint32

	// ContentType describes the content of the Will Message.
	ContentType []byte

	// ResponseTopic indicates the topic name for response message.
	ResponseTopic []byte

	// CorrelationData is used to correlate a Response Message with a Request
	// Message.
	CorrelationData []byte

	// SessionExpiryInterval represents the time, in seconds, which the broker
	// must store the Session State after the network connection is closed.
	SessionExpiryInterval *uint32

	// AuthMethod contains the name of the authentication method used for
	// extended authentication.
	AuthMethod []byte

	// AuthData contains the authentication data.
	AuthData []byte

	// RequestProblemInfo indicates whether the Reason String or User Properties
	// can be sent to the client in case of failures on any packet.
	RequestProblemInfo *bool

	// WillDelayInterval represents the time, in seconds, which the broker must
	// delay to publish the Will Message.
	WillDelayInterval *uint32

	// RequestResponseInfo indicates if the broker can send Response Information
	// with the CONNACK Packet.
	RequestResponseInfo *bool

	// ReceiveMaximum represents the maximum number of QoS 1 and QoS 2 messages
	// the client is willing to process concurrently.
	ReceiveMaximum *uint16

	// TopicAliasMaximum represents the highest value that the client will
	// accept as a Topic Alias sent by the broker.
	TopicAliasMaximum *uint16

	// UserProperties is a map of user provided properties.
	UserProperties []UserProperty

	// MaximumPacketSize represents the maximum packet size, in bytes, the
	// client is willing to accept.
	MaximumPacketSize *uint32
}

// UserProperty constains a key/value pair of a user property.
type UserProperty struct {
	// Key represents the key of the key/value pair of the property.
	Key []byte

	// Value represents the value of the key/value pair of the property.
	Value []byte
}

type propertyType byte

const (
	propertyPayloadFormat         propertyType = 0x01
	propertyMessageExpiryInterval propertyType = 0x02
	propertyContentType           propertyType = 0x03
	propertyResponseTopic         propertyType = 0x08
	propertyCorrelationData       propertyType = 0x09
	propertySessionExpiryInterval propertyType = 0x11
	propertyAuthMethod            propertyType = 0x15
	propertyAuthData              propertyType = 0x16
	propertyRequestProblemInfo    propertyType = 0x17
	propertyWillDelayInterval     propertyType = 0x18
	propertyRequestResponseInfo   propertyType = 0x19
	propertyReceiveMaximum        propertyType = 0x21
	propertyTopicAliasMaximum     propertyType = 0x22
	propertyUser                  propertyType = 0x26
	propertyMaximumPacketSize     propertyType = 0x27
)

type propertyHandler struct {
	fn    func(p *Properties, b *bytes.Buffer) error
	types map[PacketType]struct{}
}

var propertyHandlers = map[propertyType]propertyHandler{
	propertyPayloadFormat: {
		fn:    unpackPropertyPayloadFormat,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyMessageExpiryInterval: {
		fn:    unpackPropertyMessageExpiryInterval,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyContentType: {
		fn:    unpackPropertyContentType,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyResponseTopic: {
		fn:    unpackPropertyResponseTopic,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyCorrelationData: {
		fn:    unpackPropertyCorrelationData,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertySessionExpiryInterval: {
		fn:    unpackPropertySessionExpiryInterval,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyAuthMethod: {
		fn:    unpackPropertyAuthMethod,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyAuthData: {
		fn:    unpackPropertyAuthData,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyRequestProblemInfo: {
		fn:    unpackPropertyRequestProblemInfo,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyWillDelayInterval: {
		fn:    unpackPropertyWillDelayInterval,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyRequestResponseInfo: {
		fn:    unpackPropertyRequestResponseInfo,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyReceiveMaximum: {
		fn:    unpackPropertyReceiveMaximum,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyTopicAliasMaximum: {
		fn:    unpackPropertyTopicAliasMaximum,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyUser: {
		fn:    unpackPropertyUser,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
	propertyMaximumPacketSize: {
		fn:    unpackPropertyMaximumPacketSize,
		types: map[PacketType]struct{}{CONNECT: {}},
	},
}

func (p *Properties) unpack(b *bytes.Buffer, t PacketType) error {
	propsLen, err := readVariableInteger(b)
	if err != nil {
		return err
	}
	if propsLen == 0 {
		return nil
	}

	props := bytes.NewBuffer(b.Next(propsLen))
	return p.unpackProperties(props, t)
}

func (p *Properties) unpackProperties(b *bytes.Buffer, t PacketType) error {
	for {
		bt, err := b.ReadByte()
		if err != nil {
			break
		}

		pt := propertyType(bt)

		handler, ok := propertyHandlers[pt]
		if !ok || !isValidProperty(handler, t) {
			return ErrV5MalformedPacket
		}

		err = handler.fn(p, b)
		if err != nil {
			return err
		}
	}

	if p.AuthData != nil && p.AuthMethod == nil {
		return ErrV5ProtocolError
	}

	return nil
}

func isValidProperty(h propertyHandler, t PacketType) bool {
	_, ok := h.types[t]
	return ok
}

func unpackPropertyPayloadFormat(p *Properties, b *bytes.Buffer) error {
	return readPropByte(
		&p.PayloadFormatIndicator,
		b,
		func(b byte) bool { return b == 0 || b == 1 })
}

func unpackPropertyMessageExpiryInterval(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(&p.MessageExpiryInterval, b, nil)
}

func unpackPropertyContentType(p *Properties, b *bytes.Buffer) error {
	return readPropString(&p.ContentType, b)
}

func unpackPropertyResponseTopic(p *Properties, b *bytes.Buffer) error {
	return readPropString(&p.ResponseTopic, b)
}

func unpackPropertyCorrelationData(p *Properties, b *bytes.Buffer) error {
	return readPropBinary(&p.CorrelationData, b)
}

func unpackPropertySessionExpiryInterval(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(&p.SessionExpiryInterval, b, nil)
}

func unpackPropertyAuthMethod(p *Properties, b *bytes.Buffer) error {
	return readPropString(&p.AuthMethod, b)
}

func unpackPropertyAuthData(p *Properties, b *bytes.Buffer) error {
	return readPropBinary(&p.AuthData, b)
}

func unpackPropertyRequestProblemInfo(p *Properties, b *bytes.Buffer) error {
	return readPropBool(&p.RequestProblemInfo, b)
}

func unpackPropertyWillDelayInterval(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(&p.WillDelayInterval, b, nil)
}

func unpackPropertyRequestResponseInfo(p *Properties, b *bytes.Buffer) error {
	return readPropBool(&p.RequestResponseInfo, b)
}

func unpackPropertyReceiveMaximum(p *Properties, b *bytes.Buffer) error {
	return readPropUint16(
		&p.ReceiveMaximum,
		b,
		func(u uint16) bool { return u != 0 })
}

func unpackPropertyTopicAliasMaximum(p *Properties, b *bytes.Buffer) error {
	return readPropUint16(&p.TopicAliasMaximum, b, nil)
}

func unpackPropertyUser(p *Properties, b *bytes.Buffer) error {
	kv := make([][]byte, 2)

	for i := 0; i < len(kv); i++ {
		str, err := decodeString(b)
		if err != nil {
			return ErrV5MalformedPacket
		}

		kv[i] = str
	}

	p.UserProperties = append(p.UserProperties,
		UserProperty{Key: kv[0], Value: kv[1]})

	return nil
}

func unpackPropertyMaximumPacketSize(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(
		&p.MaximumPacketSize,
		b,
		func(u uint32) bool { return u != 0 })
}

func readPropByte(v **byte, b *bytes.Buffer, val func(byte) bool) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	prop, err := unpackByte(b, MQTT_V5_0)
	if err != nil {
		return err
	}

	if val != nil && !val(prop) {
		return ErrV5ProtocolError
	}

	*v = &prop
	return nil
}

func readPropBool(v **bool, b *bytes.Buffer) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	bt, err := unpackByte(b, MQTT_V5_0)
	if err != nil {
		return err
	}

	if bt > 1 {
		return ErrV5ProtocolError
	}

	prop := bt == 1
	*v = &prop
	return nil
}

func readPropUint16(v **uint16, b *bytes.Buffer, val func(uint16) bool) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	prop, err := unpackUint16(b, MQTT_V5_0)
	if err != nil {
		return err
	}

	if val != nil && !val(prop) {
		return ErrV5ProtocolError
	}

	*v = &prop
	return nil
}

func readPropUint32(v **uint32, b *bytes.Buffer, val func(uint32) bool) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	prop, err := unpackUint32(b, MQTT_V5_0)
	if err != nil {
		return err
	}

	if val != nil && !val(prop) {
		return ErrV5ProtocolError
	}

	*v = &prop
	return nil
}

func readPropString(v *[]byte, b *bytes.Buffer) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	s, err := unpackString(b, MQTT_V5_0)
	if err != nil {
		return err
	}

	*v = s
	return nil
}

func readPropBinary(v *[]byte, b *bytes.Buffer) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	bt, err := unpackBinary(b, MQTT_V5_0)
	if err != nil {
		return err
	}

	*v = bt
	return nil
}
