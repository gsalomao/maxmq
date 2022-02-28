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
	"errors"
)

const (
	connectFlagReserved     = 0x01
	connectFlagCleanSession = 0x02
	connectFlagWillFlag     = 0x04
	connectFlagWillQoSLSB   = 0x08
	connectFlagWillQoSMSB   = 0x10
	connectFlagWillRetain   = 0x20
	connectFlagPassword     = 0x40
	connectFlagUserName     = 0x80
)

// Connect represents the CONNECT Packet from MQTT specifications.
type Connect struct {
	// Version represents the MQTT version.
	Version MQTTVersion

	// CleanSession indicates if the session is temporary or not.
	CleanSession bool

	// WillFlag indicates that, if the Connect request is accepted, and the Will
	// Message MUST be stored on the broker and associated with the connection.
	WillFlag bool

	// WillQoS indicates the QoS level to be used when publishing the Will
	// Message.
	WillQoS WillQoS

	// WillRetain indicates if the Will Message is to be Retained when it is
	// published.
	WillRetain bool

	// UserNameFlag indicates if the UserName is present on the message or not.
	UserNameFlag bool

	// PasswordFlag indicates if the Password is present on the message or not.
	PasswordFlag bool

	// KeepAlive is a time interval, measured in seconds, that is permitted to
	// elapse between the point at which the Client finishes transmitting one
	// Control Packet and the point it starts sending the next.
	KeepAlive uint16

	// Properties represents the CONNECT properties.
	Properties *Properties

	// ClientID identifies the client to the broker.
	ClientID []byte

	// WillProperties defines the Application Message properties to be sent with
	// the Will Message when it is published.
	WillProperties *Properties

	// WillTopic represents the topic which the Will Message will be published.
	WillTopic []byte

	// WillMessage represents the Will Message to be published.
	WillMessage []byte

	// UserName represents the UserName which the broker must use for
	// authentication and authorization.
	UserName []byte

	// Password represents the Password which the broker must use for
	// authentication and authorization.
	Password []byte

	size int
}

// WillQoS indicates the QoS level to be used when publishing the Will Message.
type WillQoS byte

// Will QoS level.
const (
	WillQoS0 WillQoS = iota
	WillQoS1
	WillQoS2
)

var protocolNames = map[MQTTVersion][]byte{
	MQTT31:  {'M', 'Q', 'I', 's', 'd', 'p'},
	MQTT311: {'M', 'Q', 'T', 'T'},
	MQTT50:  {'M', 'Q', 'T', 'T'},
}

func newPacketConnect(fh fixedHeader) (Packet, error) {
	if fh.packetType != CONNECT {
		return nil, errors.New("packet type is not CONNECT")
	}

	if fh.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (CONNECT)")
	}

	return &Connect{size: fh.size + fh.remainingLength}, nil
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the CONNECT Packet in this broker.
func (pkt *Connect) Pack(_ *bufio.Writer) error {
	return errors.New("unsupported (CONNECT)")
}

// Unpack reads the packet bytes from bytes.Buffer and decodes them into the
// packet.
func (pkt *Connect) Unpack(buf *bytes.Buffer) error {
	err := pkt.unpackVersion(buf)
	if err != nil {
		return err
	}

	err = pkt.unpackFlags(buf)
	if err != nil {
		return err
	}

	pkt.KeepAlive, err = readUint16(buf, pkt.Version)
	if err != nil {
		return err
	}

	err = pkt.unpackProperties(buf)
	if err != nil {
		return err
	}

	err = pkt.unpackClientID(buf) // MQTT-3.1.3-3
	if err != nil {
		return err
	}

	err = pkt.unpackWill(buf)
	if err != nil {
		return err
	}

	err = pkt.unpackUserName(buf)
	if err != nil {
		return err
	}

	err = pkt.unpackPassword(buf)
	if err != nil {
		return err
	}

	return nil
}

// Type returns the packet type.
func (pkt *Connect) Type() Type {
	return CONNECT
}

// Size returns the packet size in bytes.
func (pkt *Connect) Size() int {
	return pkt.size
}

func (pkt *Connect) unpackVersion(buf *bytes.Buffer) error {
	// As the MQTT version is unknown yet, use the default version
	protoName, err := readString(buf, MQTT311)
	if err != nil {
		return errors.New("cannot decode protocol name (CONNECT)")
	}

	v, err := buf.ReadByte()
	pkt.Version = MQTTVersion(v)
	if err != nil {
		return errors.New("no protocol version (CONNECT)")
	}

	if n, ok := protocolNames[pkt.Version]; !ok { // MQTT-3.1.2-2
		return ErrV3UnacceptableProtocolVersion
	} else if !bytes.Equal(n, protoName) { // MQTT-3.1.2-1
		return errors.New("invalid protocol name (CONNECT)")
	}

	return nil
}

func (pkt *Connect) unpackFlags(buf *bytes.Buffer) error {
	flags, err := buf.ReadByte()
	if err != nil {
		return errors.New("no Connect Flags (CONNECT)")
	}

	if hasFlag(flags, connectFlagReserved) { // MQTT-3.1.2-3
		return newErrMalformedPacket(pkt.Version,
			"invalid Connect Flags (CONNECT)")
	}

	pkt.CleanSession = hasFlag(flags, connectFlagCleanSession)

	err = pkt.unpackFlagsWill(flags)
	if err != nil {
		return err
	}

	err = pkt.unpackFlagsUserNamePassword(flags)
	if err != nil {
		return err
	}

	return nil
}

func (pkt *Connect) unpackFlagsWill(flags byte) error {
	pkt.WillFlag = hasFlag(flags, connectFlagWillFlag)

	wQoS := flags & (connectFlagWillQoSMSB | connectFlagWillQoSLSB) >> 3

	pkt.WillQoS = WillQoS(wQoS)
	if !pkt.WillFlag && pkt.WillQoS != WillQoS0 { // MQTT-3.1.2-13
		return newErrMalformedPacket(pkt.Version,
			"invalid Connect Flags (CONNECT)")
	}

	if pkt.WillQoS > WillQoS2 { // MQTT-3.1.2-14
		return newErrMalformedPacket(pkt.Version,
			"invalid Connect Flags (CONNECT)")
	}

	pkt.WillRetain = hasFlag(flags, connectFlagWillRetain)
	if !pkt.WillFlag && pkt.WillRetain { // MQTT-3.1.2-15
		return newErrMalformedPacket(pkt.Version,
			"invalid Connect Flags (CONNECT)")
	}

	return nil
}

func (pkt *Connect) unpackFlagsUserNamePassword(flags byte) error {
	pkt.PasswordFlag = hasFlag(flags, connectFlagPassword)
	pkt.UserNameFlag = hasFlag(flags, connectFlagUserName)

	if pkt.PasswordFlag && !pkt.UserNameFlag { // MQTT-3.1.2-22
		return newErrMalformedPacket(pkt.Version,
			"invalid Connect Flags (CONNECT)")
	}

	return nil
}

func (pkt *Connect) unpackProperties(buf *bytes.Buffer) error {
	if pkt.Version != MQTT50 {
		return nil
	}

	var err error

	pkt.Properties = &Properties{}

	err = pkt.Properties.unpack(buf, CONNECT)
	if err != nil {
		return err
	}

	return nil
}

func (pkt *Connect) unpackClientID(buf *bytes.Buffer) error {
	var err error

	pkt.ClientID, err = readString(buf, pkt.Version) // MQTT-3.1.3-2
	if err != nil {
		return newErrMalformedPacket(pkt.Version, "invalid ClientID (CONNECT)")
	}

	if len(pkt.ClientID) == 0 {
		if pkt.Version == MQTT31 {
			return ErrV3IdentifierRejected
		}
		if pkt.Version == MQTT311 && !pkt.CleanSession { // MQTT-3.1.3-7
			return ErrV3IdentifierRejected
		}
	}

	return nil
}

func (pkt *Connect) unpackWill(buf *bytes.Buffer) error {
	if !pkt.WillFlag { // MQTT-3.1.2-8
		return nil
	}

	var err error

	if pkt.Version == MQTT50 {
		pkt.WillProperties = &Properties{}

		err = pkt.WillProperties.unpack(buf, CONNECT)
		if err != nil {
			return err
		}
	}

	pkt.WillTopic, err = readString(buf, pkt.Version) // MQTT-3.1.3-10
	if err != nil {
		return newErrMalformedPacket(pkt.Version,
			"invalid Will Topic (CONNECT)")
	}

	pkt.WillMessage, err = readString(buf, pkt.Version)
	if err != nil {
		return newErrMalformedPacket(pkt.Version,
			"invalid Will Message (CONNECT)")
	}

	return nil
}

func (pkt *Connect) unpackUserName(buf *bytes.Buffer) error {
	if !pkt.UserNameFlag { // MQTT-3.1.2-18, MQTT-3.1.2-19
		return nil
	}

	var err error

	pkt.UserName, err = readString(buf, pkt.Version) // MQTT-3.1.3-11
	if err != nil {
		return newErrMalformedPacket(pkt.Version, "invalid User Name (CONNECT)")
	}

	return nil
}

func (pkt *Connect) unpackPassword(buf *bytes.Buffer) error {
	if !pkt.PasswordFlag { // MQTT-3.1.2-20, MQTT-3.1.2-21
		return nil
	}

	var err error

	pkt.Password, err = readBinary(buf, pkt.Version)
	if err != nil {
		return newErrMalformedPacket(pkt.Version, "invalid Password (CONNECT)")
	}

	return nil
}

func hasFlag(flags byte, mask int) bool {
	return flags&byte(mask) > 0
}
