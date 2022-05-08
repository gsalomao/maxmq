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

	// TopicAlias represents a value that is used to identify the Topic Name.
	TopicAlias *uint16

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
	propTopicAlias                    propType = 0x23
	propMaximumQoS                    propType = 0x24
	propRetainAvailable               propType = 0x25
	propUser                          propType = 0x26
	propMaximumPacketSize             propType = 0x27
	propWildcardSubscriptionAvailable propType = 0x28
	propSubscriptionIDAvailable       propType = 0x29
	propSharedSubscriptionAvailable   propType = 0x2A
)

type propertyHandler struct {
	types map[Type]struct{}
	read  func(b *bytes.Buffer, p *Properties) error
}

var propertyHandlers = map[propType]propertyHandler{
	propPayloadFormatIndicator: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropPayloadFormat,
	},
	propMessageExpiryInterval: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropMessageExpInterval,
	},
	propContentType: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropContentType,
	},
	propResponseTopic: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropResponseTopic,
	},
	propCorrelationData: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropCorrelationData,
	},
	propSessionExpiryInterval: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}, DISCONNECT: {}},
		read:  readPropSessionExpiryInterval,
	},
	propAssignedClientID: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propServerKeepAlive: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propAuthMethod: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		read:  readPropAuthMethod,
	},
	propAuthData: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		read:  readPropAuthData,
	},
	propRequestProblemInfo: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropRequestProblemInfo,
	},
	propWillDelayInterval: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropWillDelayInterval,
	},
	propRequestResponseInfo: {
		types: map[Type]struct{}{CONNECT: {}},
		read:  readPropRequestResponseInfo,
	},
	propResponseInfo: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propServerReference: {
		types: map[Type]struct{}{CONNACK: {}, DISCONNECT: {}},
		read:  readServerReference,
	},
	propReasonString: {
		types: map[Type]struct{}{CONNACK: {}, DISCONNECT: {}},
		read:  readPropReasonString,
	},
	propReceiveMaximum: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		read:  readPropReceiveMaximum,
	},
	propTopicAliasMaximum: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		read:  readPropTopicAliasMaximum,
	},
	propTopicAlias: {
		types: map[Type]struct{}{PUBLISH: {}},
	},
	propMaximumQoS: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propRetainAvailable: {
		types: map[Type]struct{}{CONNACK: {}},
	},
	propUser: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}, DISCONNECT: {}},
		read:  readPropUser,
	},
	propMaximumPacketSize: {
		types: map[Type]struct{}{CONNECT: {}, CONNACK: {}},
		read:  readPropMaxPacketSize,
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

func writeProperties(buf *bytes.Buffer, p *Properties, t Type) error {
	if p == nil {
		return writeVarInteger(buf, 0)
	}

	b := bytes.Buffer{}
	pw := propertiesWriter{buf: &b}

	err := pw.load(p, t)
	if err != nil {
		return pw.err
	}

	_ = writeVarInteger(buf, b.Len())
	_, err = b.WriteTo(buf)
	return err
}

func readProperties(b *bytes.Buffer, t Type) (*Properties, error) {
	var propsLen int
	_, err := readVarInteger(b, &propsLen)
	if err != nil {
		return nil, err
	}
	if propsLen == 0 {
		return nil, nil
	}

	p := &Properties{}
	props := bytes.NewBuffer(b.Next(propsLen))

	err = p.unpackProperties(props, t)
	if err != nil {
		return nil, err
	}

	return p, nil
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

		err = handler.read(b, p)
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

// Reset clears all properties.
func (p *Properties) Reset() {
	p.PayloadFormatIndicator = nil
	p.MessageExpiryInterval = nil
	p.ContentType = nil
	p.ResponseTopic = nil
	p.CorrelationData = nil
	p.SessionExpiryInterval = nil
	p.AssignedClientID = nil
	p.ServerKeepAlive = nil
	p.AuthMethod = nil
	p.AuthData = nil
	p.RequestProblemInfo = nil
	p.WillDelayInterval = nil
	p.RequestResponseInfo = nil
	p.ResponseInfo = nil
	p.ServerReference = nil
	p.ReasonString = nil
	p.ReceiveMaximum = nil
	p.TopicAliasMaximum = nil
	p.TopicAlias = nil
	p.MaximumQoS = nil
	p.RetainAvailable = nil
	p.UserProperties = nil
	p.MaximumPacketSize = nil
	p.WildcardSubscriptionAvailable = nil
	p.SubscriptionIDAvailable = nil
	p.SharedSubscriptionAvailable = nil
}

func isValidProperty(h propertyHandler, t Type) bool {
	_, ok := h.types[t]
	return ok
}

func readPropPayloadFormat(b *bytes.Buffer, p *Properties) error {
	return readPropByte(
		b,
		&p.PayloadFormatIndicator,
		func(b byte) bool { return b == 0 || b == 1 },
	)
}

func readPropMessageExpInterval(b *bytes.Buffer, p *Properties) error {
	return readPropUint32(b, &p.MessageExpiryInterval, nil)
}

func readPropContentType(b *bytes.Buffer, p *Properties) error {
	return readPropertyString(b, &p.ContentType)
}

func readPropResponseTopic(b *bytes.Buffer, p *Properties) error {
	return readPropertyString(b, &p.ResponseTopic)
}

func readPropCorrelationData(b *bytes.Buffer, p *Properties) error {
	return readPropertyBinary(b, &p.CorrelationData)
}

func readPropSessionExpiryInterval(b *bytes.Buffer, p *Properties) error {
	return readPropUint32(b, &p.SessionExpiryInterval, nil)
}

func readPropAuthMethod(b *bytes.Buffer, p *Properties) error {
	return readPropertyString(b, &p.AuthMethod)
}

func readPropAuthData(b *bytes.Buffer, p *Properties) error {
	return readPropertyBinary(b, &p.AuthData)
}

func readPropRequestProblemInfo(b *bytes.Buffer, p *Properties) error {
	return readPropByte(
		b,
		&p.RequestProblemInfo,
		func(b byte) bool { return b == 0 || b == 1 },
	)
}

func readPropWillDelayInterval(b *bytes.Buffer, p *Properties) error {
	return readPropUint32(b, &p.WillDelayInterval, nil)
}

func readPropRequestResponseInfo(b *bytes.Buffer, p *Properties) error {
	return readPropByte(
		b,
		&p.RequestResponseInfo,
		func(b byte) bool { return b == 0 || b == 1 },
	)
}

func readPropReceiveMaximum(b *bytes.Buffer, p *Properties) error {
	return readPropUint16(
		b,
		&p.ReceiveMaximum,
		func(u uint16) bool { return u != 0 },
	)
}

func readPropTopicAliasMaximum(b *bytes.Buffer, p *Properties) error {
	return readPropUint16(b, &p.TopicAliasMaximum, nil)
}

func readPropUser(b *bytes.Buffer, p *Properties) error {
	kv := make([][]byte, 2)

	for i := 0; i < len(kv); i++ {
		s, err := readString(b, MQTT50)
		if err != nil {
			return err
		}

		kv[i] = s
	}

	p.UserProperties = append(p.UserProperties,
		UserProperty{Key: kv[0], Value: kv[1]})

	return nil
}

func readPropMaxPacketSize(b *bytes.Buffer, p *Properties) error {
	return readPropUint32(
		b,
		&p.MaximumPacketSize,
		func(u uint32) bool { return u != 0 },
	)
}

func readPropReasonString(b *bytes.Buffer, p *Properties) error {
	return readPropertyString(b, &p.ReasonString)
}

func readServerReference(b *bytes.Buffer, p *Properties) error {
	return readPropertyString(b, &p.ServerReference)
}

func readPropByte(b *bytes.Buffer, v **byte, val func(byte) bool) error {
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

func readPropUint16(b *bytes.Buffer, v **uint16, val func(uint16) bool) error {
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

func readPropUint32(b *bytes.Buffer, v **uint32, val func(uint32) bool) error {
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

func readPropertyString(b *bytes.Buffer, buf *[]byte) error {
	if *buf != nil {
		return ErrV5ProtocolError
	}

	var err error
	*buf, err = readString(b, MQTT50)
	return err
}

func readPropertyBinary(b *bytes.Buffer, buf *[]byte) error {
	if *buf != nil {
		return ErrV5ProtocolError
	}

	var err error
	*buf, err = readBinary(b, MQTT50)
	return err
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
	w.writeUint16(p.TopicAlias, propTopicAlias, t)
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
