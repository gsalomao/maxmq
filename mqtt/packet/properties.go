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
	"bufio"
	"bytes"
	"fmt"
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

	// AssignedClientID represents the client ID assigned by the broker in case
	// of the client connected with the broker without specifying a client ID.
	AssignedClientID []byte

	// ServerKeepAlive represents the Keep Alive, in seconds, assigned by the
	// broker, and to be used by the client.
	ServerKeepAlive *uint16

	// AuthMethod contains the name of the authentication method.
	AuthMethod []byte

	// AuthData contains the authentication data.
	AuthData []byte

	// RequestProblemInfo indicates whether the Reason String or User Properties
	// can be sent to the client in case of failures on any packet.
	RequestProblemInfo *byte

	// WillDelayInterval represents the time, in seconds, which the broker must
	// delay publishing the Will Message.
	WillDelayInterval *uint32

	// RequestResponseInfo indicates if the broker can send Response Information
	// with the CONNACK Packet.
	RequestResponseInfo *byte

	// ResponseInfo contains a string that can be used to as the basis for
	// creating a Response Topic.
	ResponseInfo []byte

	// ServerReference contains a string indicating another broker the client
	// can use.
	ServerReference []byte

	// ReasonString represents the reason associated with the response.
	ReasonString []byte

	// ReceiveMaximum represents the maximum number of in-flight messages with
	// QoS > 0.
	ReceiveMaximum *uint16

	// TopicAliasMaximum represents the highest number of Topic Alias that the
	// client or the broker accepts.
	TopicAliasMaximum *uint16

	// MaximumQoS represents the maximum QoS supported by the broker.
	MaximumQoS *byte

	// RetainAvailable indicates whether the broker supports retained messages
	// or not.
	RetainAvailable *byte

	// UserProperties is a map of user provided properties.
	UserProperties []UserProperty

	// MaximumPacketSize represents the maximum packet size, in bytes, the
	// client or the broker is willing to accept.
	MaximumPacketSize *uint32

	// WildcardSubscriptionAvailable indicates whether the broker supports
	// Wildcard Subscriptions or not.
	WildcardSubscriptionAvailable *byte

	// SubscriptionIDAvailable indicates whether the broker supports
	// Subscription Identifiers or not.
	SubscriptionIDAvailable *byte

	// SharedSubscriptionAvailable indicates whether the broker supports Shared
	// Subscriptions or not.
	SharedSubscriptionAvailable *byte
}

// UserProperty contains the key/value pair to a user property.
type UserProperty struct {
	// Key represents the key of the key/value pair to the property.
	Key []byte

	// Value represents the value of the key/value pair to the property.
	Value []byte
}

type propType byte

const (
	propPayloadFormatIndicator        propType = 0x01
	propMessageExpiryInterval         propType = 0x02
	propContentType                   propType = 0x03
	propResponseTopic                 propType = 0x08
	propCorrelationData               propType = 0x09
	propSessionExpiryInterval         propType = 0x11
	propAssignedClientID              propType = 0x12
	propServerKeepAlive               propType = 0x13
	propAuthMethod                    propType = 0x15
	propAuthData                      propType = 0x16
	propRequestProblemInfo            propType = 0x17
	propWillDelayInterval             propType = 0x18
	propRequestResponseInfo           propType = 0x19
	propResponseInfo                  propType = 0x1A
	propServerReference               propType = 0x1C
	propReasonString                  propType = 0x1F
	propReceiveMaximum                propType = 0x21
	propTopicAliasMaximum             propType = 0x22
	propMaximumQoS                    propType = 0x24
	propRetainAvailable               propType = 0x25
	propUser                          propType = 0x26
	propMaximumPacketSize             propType = 0x27
	propWildcardSubscriptionAvailable propType = 0x28
	propSubscriptionIDAvailable       propType = 0x29
	propSharedSubscriptionAvailable   propType = 0x2A
)

type propertyHandler struct {
	types  map[Type]struct{}
	unpack func(p *Properties, b *bytes.Buffer) error
}

var propertyHandlers = map[propType]propertyHandler{
	propPayloadFormatIndicator: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropPayloadFormat,
	},
	propMessageExpiryInterval: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropMessageExpiryInterval,
	},
	propContentType: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropContentType,
	},
	propResponseTopic: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropResponseTopic,
	},
	propCorrelationData: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropCorrelationData,
	},
	propSessionExpiryInterval: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}, DISCONNECT: {}},
		unpack: unpackPropSessionExpiryInterval,
	},
	propAssignedClientID: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propServerKeepAlive: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propAuthMethod: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		unpack: unpackPropAuthMethod,
	},
	propAuthData: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		unpack: unpackPropAuthData,
	},
	propRequestProblemInfo: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropRequestProblemInfo,
	},
	propWillDelayInterval: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropWillDelayInterval,
	},
	propRequestResponseInfo: {
		types:  map[Type]struct{}{CONNECT: {}},
		unpack: unpackPropRequestResponseInfo,
	},
	propResponseInfo: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propServerReference: {
		types: map[Type]struct{}{CONNACK: {}, DISCONNECT: {}},
	},
	propReasonString: {
		types: map[Type]struct{}{CONNACK: {}, DISCONNECT: {}},
	},
	propReceiveMaximum: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		unpack: unpackPropReceiveMaximum,
	},
	propTopicAliasMaximum: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		unpack: unpackPropTopicAliasMaximum,
	},
	propMaximumQoS: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propRetainAvailable: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propUser: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}, DISCONNECT: {}},
		unpack: unpackPropertyUser,
	},
	propMaximumPacketSize: {
		types:  map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		unpack: unpackPropertyMaximumPacketSize,
	},
	propWildcardSubscriptionAvailable: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propSubscriptionIDAvailable: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propSharedSubscriptionAvailable: {
		types: map[Type]struct{}{CONNACK: {}},
	},
}

type propertiesWriter struct {
	buf *bytes.Buffer
	err error
}

func (w *propertiesWriter) load(p *Properties, t Type) error {
	w.writeUint32(p.SessionExpiryInterval, propSessionExpiryInterval, t)
	w.writeUint16(p.ReceiveMaximum, propReceiveMaximum, t)
	w.writeByte(p.MaximumQoS, propMaximumQoS, t)
	w.writeByte(p.RetainAvailable, propRetainAvailable, t)
	w.writeUint32(p.MaximumPacketSize, propMaximumPacketSize, t)
	w.writeBinary(p.AssignedClientID, propAssignedClientID, t)
	w.writeUint16(p.TopicAliasMaximum, propTopicAliasMaximum, t)
	w.writeBinary(p.ReasonString, propReasonString, t)
	w.writeUserProperties(p.UserProperties, t)
	w.writeByte(p.WildcardSubscriptionAvailable,
		propWildcardSubscriptionAvailable, t)
	w.writeByte(p.SubscriptionIDAvailable, propSubscriptionIDAvailable, t)
	w.writeByte(p.SharedSubscriptionAvailable, propSharedSubscriptionAvailable,
		t)
	w.writeUint16(p.ServerKeepAlive, propServerKeepAlive, t)
	w.writeBinary(p.ResponseInfo, propResponseInfo, t)
	w.writeBinary(p.ServerReference, propServerReference, t)
	w.writeBinary(p.AuthMethod, propAuthMethod, t)
	w.writeBinary(p.AuthData, propAuthData, t)

	return w.err
}

func (w *propertiesWriter) isValid(pt propType, t Type) bool {
	if w.err != nil {
		return false
	}

	handler, ok := propertyHandlers[pt]
	if !ok || !isValidProperty(handler, t) {
		w.err = fmt.Errorf("invalid packet type (%s) or property (0x%X)",
			t.String(), pt)
		return false
	}

	return true
}

func (w *propertiesWriter) writeByte(v *byte, pt propType, t Type) {
	if v == nil || !w.isValid(pt, t) {
		return
	}

	w.buf.WriteByte(byte(pt))
	w.buf.WriteByte(*v)
}

func (w *propertiesWriter) writeUint16(v *uint16, pt propType, t Type) {
	if v == nil || !w.isValid(pt, t) {
		return
	}

	w.buf.WriteByte(byte(pt))
	writeUint16(w.buf, *v)
}

func (w *propertiesWriter) writeUint32(v *uint32, pt propType, t Type) {
	if v == nil || !w.isValid(pt, t) {
		return
	}

	w.buf.WriteByte(byte(pt))
	writeUint32(w.buf, *v)
}

func (w *propertiesWriter) writeBinary(v []byte, pt propType, t Type) {
	if v == nil || !w.isValid(pt, t) {
		return
	}

	w.buf.WriteByte(byte(pt))
	writeBinary(w.buf, v)
}

func (w *propertiesWriter) writeUserProperties(v []UserProperty, t Type) {
	if len(v) == 0 || !w.isValid(propUser, t) {
		return
	}

	for _, u := range v {
		w.buf.WriteByte(byte(propUser))
		writeBinary(w.buf, u.Key)
		writeBinary(w.buf, u.Value)
	}
}

func (p *Properties) pack(buf *bytes.Buffer, t Type) error {
	b := bytes.Buffer{}
	pw := propertiesWriter{buf: &b}

	err := pw.load(p, t)
	if err != nil {
		return pw.err
	}

	wr := bufio.NewWriterSize(buf, 4 /* max variable integer size */)
	_ = writeVarInteger(wr, b.Len())

	err = wr.Flush()
	if err != nil {
		return err
	}

	_, err = b.WriteTo(buf)
	return err
}

func (p *Properties) unpack(b *bytes.Buffer, t Type) error {
	var propsLen int
	_, err := readVarInteger(b, &propsLen)
	if err != nil {
		return err
	}
	if propsLen == 0 {
		return nil
	}

	props := bytes.NewBuffer(b.Next(propsLen))
	return p.unpackProperties(props, t)
}

func (p *Properties) unpackProperties(b *bytes.Buffer, t Type) error {
	for {
		bt, err := b.ReadByte()
		if err != nil {
			break
		}

		pt := propType(bt)

		handler, ok := propertyHandlers[pt]
		if !ok || !isValidProperty(handler, t) {
			return ErrV5MalformedPacket
		}

		err = handler.unpack(p, b)
		if err != nil {
			return err
		}
	}

	if p.AuthData != nil && p.AuthMethod == nil {
		return ErrV5ProtocolError
	}

	return nil
}

func (p *Properties) size(t Type) int {
	b := bytes.Buffer{}
	pw := propertiesWriter{buf: &b}

	err := pw.load(p, t)
	if err != nil {
		return 0
	}

	return b.Len()
}

func isValidProperty(h propertyHandler, t Type) bool {
	_, ok := h.types[t]
	return ok
}

func unpackPropPayloadFormat(p *Properties, b *bytes.Buffer) error {
	return readPropByte(
		&p.PayloadFormatIndicator,
		b,
		func(b byte) bool { return b == 0 || b == 1 },
	)
}

func unpackPropMessageExpiryInterval(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(&p.MessageExpiryInterval, b, nil)
}

func unpackPropContentType(p *Properties, b *bytes.Buffer) error {
	return readPropString(&p.ContentType, b)
}

func unpackPropResponseTopic(p *Properties, b *bytes.Buffer) error {
	return readPropString(&p.ResponseTopic, b)
}

func unpackPropCorrelationData(p *Properties, b *bytes.Buffer) error {
	return readPropBinary(&p.CorrelationData, b)
}

func unpackPropSessionExpiryInterval(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(&p.SessionExpiryInterval, b, nil)
}

func unpackPropAuthMethod(p *Properties, b *bytes.Buffer) error {
	return readPropString(&p.AuthMethod, b)
}

func unpackPropAuthData(p *Properties, b *bytes.Buffer) error {
	return readPropBinary(&p.AuthData, b)
}

func unpackPropRequestProblemInfo(p *Properties, b *bytes.Buffer) error {
	return readPropByte(
		&p.RequestProblemInfo,
		b,
		func(b byte) bool { return b == 0 || b == 1 },
	)
}

func unpackPropWillDelayInterval(p *Properties, b *bytes.Buffer) error {
	return readPropUint32(&p.WillDelayInterval, b, nil)
}

func unpackPropRequestResponseInfo(p *Properties, b *bytes.Buffer) error {
	return readPropByte(
		&p.RequestResponseInfo,
		b,
		func(b byte) bool { return b == 0 || b == 1 },
	)
}

func unpackPropReceiveMaximum(p *Properties, b *bytes.Buffer) error {
	return readPropUint16(
		&p.ReceiveMaximum,
		b,
		func(u uint16) bool { return u != 0 },
	)
}

func unpackPropTopicAliasMaximum(p *Properties, b *bytes.Buffer) error {
	return readPropUint16(&p.TopicAliasMaximum, b, nil)
}

func unpackPropertyUser(p *Properties, b *bytes.Buffer) error {
	kv := make([][]byte, 2)

	for i := 0; i < len(kv); i++ {
		str, err := readString(b, MQTT50)
		if err != nil {
			return err
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
		func(u uint32) bool { return u != 0 },
	)
}

func readPropByte(v **byte, b *bytes.Buffer, val func(byte) bool) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	prop, err := b.ReadByte()
	if err != nil {
		return ErrV5MalformedPacket
	}

	if val != nil && !val(prop) {
		return ErrV5ProtocolError
	}

	*v = &prop
	return nil
}

func readPropUint16(v **uint16, b *bytes.Buffer, val func(uint16) bool) error {
	if *v != nil {
		return ErrV5ProtocolError
	}

	prop, err := readUint16(b, MQTT50)
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

	prop, err := readUint32(b, MQTT50)
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

	s, err := readString(b, MQTT50)
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

	bt, err := readBinary(b, MQTT50)
	if err != nil {
		return err
	}

	*v = bt
	return nil
}
