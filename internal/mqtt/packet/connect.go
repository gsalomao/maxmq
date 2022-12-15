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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
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
	// ClientID identifies the client to the broker.
	ClientID []byte

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

	// Properties represents the CONNECT properties (MQTT V5.0 only).
	Properties *Properties

	// WillProperties defines the Application Message properties to be sent with
	// the Will Message when it is published.
	WillProperties *Properties

	// KeepAlive is a time interval, measured in seconds, that is permitted to
	// elapse between the point at which the Client finishes transmitting one
	// Control Packet and the point it starts sending the next.
	KeepAlive uint16

	// Version represents the MQTT version.
	Version MQTTVersion

	// WillQoS indicates the QoS level to be used when publishing the Will
	// Message.
	WillQoS WillQoS

	// CleanSession indicates if the session is temporary or not.
	CleanSession bool

	// WillFlag indicates that, if the Connect request is accepted, and the Will
	// Message MUST be stored on the broker and associated with the connection.
	WillFlag bool

	// WillRetain indicates if the Will Message is to be Retained when it is
	// published.
	WillRetain bool

	// UserNameFlag indicates if the UserName is present on the message or not.
	UserNameFlag bool

	// PasswordFlag indicates if the Password is present on the message or not.
	PasswordFlag bool

	// Unexported fields
	timestamp    time.Time
	size         int
	remainLength int
}

// WillQoS indicates the QoS level to be used when publishing the Will Message.
type WillQoS byte

// Will QoS level.
const (
	WillQoS0 WillQoS = iota
	WillQoS1
	WillQoS2
)

type protocolName []byte

var protocolNames = map[MQTTVersion]protocolName{
	MQTT31:  {'M', 'Q', 'I', 's', 'd', 'p'},
	MQTT311: {'M', 'Q', 'T', 'T'},
	MQTT50:  {'M', 'Q', 'T', 'T'},
}

func newPacketConnect(opts options) (Packet, error) {
	if opts.packetType != CONNECT {
		return nil, errors.New("packet type is not CONNECT")
	}

	if opts.controlFlags != 0 {
		return nil, errors.New("invalid Control Flags (CONNECT)")
	}

	return &Connect{
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// Write encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the CONNECT Packet.
func (pkt *Connect) Write(_ *bufio.Writer) error {
	return errors.New("unsupported (CONNECT)")
}

// Read reads the packet bytes from bufio.Reader and decodes them into the
// packet.
func (pkt *Connect) Read(r *bufio.Reader) error {
	msg := make([]byte, pkt.remainLength)
	if _, err := io.ReadFull(r, msg); err != nil {
		return fmt.Errorf("failed to read remaining bytes: %w", err)
	}
	buf := bytes.NewBuffer(msg)

	name, err := pkt.readProtocolName(buf)
	if err != nil {
		return fmt.Errorf("failed to read protocol name: %w", err)
	}

	err = pkt.readVersion(buf, name)
	if err != nil {
		return fmt.Errorf("failed to read protocol version: %w", err)
	}

	err = pkt.readFlags(buf)
	if err != nil {
		return fmt.Errorf("failed to read flags: %w", err)
	}

	pkt.KeepAlive, err = readUint[uint16](buf)
	if err != nil {
		return fmt.Errorf("failed to read keep alive: %w", err)
	}

	err = pkt.readProperties(buf)
	if err != nil {
		return fmt.Errorf("failed to read properties: %w", err)
	}

	err = pkt.readClientID(buf) // MQTT-3.1.3-3
	if err != nil {
		return fmt.Errorf("failed to read client ID: %w", err)
	}

	err = pkt.readWill(buf)
	if err != nil {
		return err
	}

	err = pkt.readUserName(buf)
	if err != nil {
		return fmt.Errorf("failed to read username: %w", err)
	}

	err = pkt.readPassword(buf)
	if err != nil {
		return fmt.Errorf("failed to read password: %w", err)
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

// Timestamp returns the timestamp of the moment which the packet has been
// received.
func (pkt *Connect) Timestamp() time.Time {
	return pkt.timestamp
}

func (pkt *Connect) readProtocolName(buf *bytes.Buffer) (protocolName, error) {
	name, err := readString(buf)
	if err != nil {
		return nil, err
	}
	return name, nil
}

func (pkt *Connect) readVersion(buf *bytes.Buffer, name protocolName) error {
	v, err := buf.ReadByte()
	if err != nil {
		return err
	}
	pkt.Version = MQTTVersion(v)

	if n, ok := protocolNames[pkt.Version]; !ok { // MQTT-3.1.2-2
		return ErrV3UnacceptableProtocolVersion
	} else if !bytes.Equal(n, name) { // MQTT-3.1.2-1
		return newErrMalformedPacket("invalid protocol name")
	}

	return nil
}

func (pkt *Connect) readFlags(buf *bytes.Buffer) error {
	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}

	if hasFlag(flags, connectFlagReserved) { // MQTT-3.1.2-3
		return newErrMalformedPacket("invalid reserved flag")
	}

	pkt.CleanSession = hasFlag(flags, connectFlagCleanSession)

	err = pkt.readFlagsWill(flags)
	if err != nil {
		return err
	}

	err = pkt.readFlagsUserNamePassword(flags)
	if err != nil {
		return err
	}

	return nil
}

func (pkt *Connect) readFlagsWill(flags byte) error {
	pkt.WillFlag = hasFlag(flags, connectFlagWillFlag)
	wQoS := flags & (connectFlagWillQoSMSB | connectFlagWillQoSLSB) >> 3

	pkt.WillQoS = WillQoS(wQoS)
	if !pkt.WillFlag && pkt.WillQoS != WillQoS0 { // MQTT-3.1.2-13
		return newErrMalformedPacket("invalid Will flag")
	}

	if pkt.WillQoS > WillQoS2 { // MQTT-3.1.2-14
		return newErrMalformedPacket("invalid Will QoS flag")
	}

	pkt.WillRetain = hasFlag(flags, connectFlagWillRetain)
	if !pkt.WillFlag && pkt.WillRetain { // MQTT-3.1.2-15
		return newErrMalformedPacket("invalid Will Retain flag")
	}

	return nil
}

func (pkt *Connect) readFlagsUserNamePassword(flags byte) error {
	pkt.PasswordFlag = hasFlag(flags, connectFlagPassword)
	pkt.UserNameFlag = hasFlag(flags, connectFlagUserName)

	if pkt.PasswordFlag && !pkt.UserNameFlag { // MQTT-3.1.2-22
		return newErrMalformedPacket("invalid username/password flag")
	}

	return nil
}

func (pkt *Connect) readProperties(buf *bytes.Buffer) error {
	if pkt.Version != MQTT50 {
		return nil
	}

	var err error
	pkt.Properties, err = readProperties(buf, CONNECT)
	if err != nil {
		return err
	}

	return nil
}

func (pkt *Connect) readClientID(buf *bytes.Buffer) error {
	var err error

	pkt.ClientID, err = readString(buf) // MQTT-3.1.3-2
	if err != nil {
		return err
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

func (pkt *Connect) readWill(buf *bytes.Buffer) error {
	if !pkt.WillFlag { // MQTT-3.1.2-8
		return nil
	}

	var err error
	if pkt.Version == MQTT50 {
		pkt.WillProperties, err = readProperties(buf, CONNECT)
		if err != nil {
			return fmt.Errorf("failed to read Will properties: %w", err)
		}
	}

	pkt.WillTopic, err = readString(buf) // MQTT-3.1.3-10
	if err != nil {
		return fmt.Errorf("failed to read Will topic: %w", err)
	}

	pkt.WillMessage, err = readString(buf)
	if err != nil {
		return fmt.Errorf("failed to read Will message: %w", err)
	}

	return nil
}

func (pkt *Connect) readUserName(buf *bytes.Buffer) error {
	if !pkt.UserNameFlag { // MQTT-3.1.2-18, MQTT-3.1.2-19
		return nil
	}

	var err error
	pkt.UserName, err = readString(buf) // MQTT-3.1.3-11
	if err != nil {
		return err
	}

	return nil
}

func (pkt *Connect) readPassword(buf *bytes.Buffer) error {
	if !pkt.PasswordFlag { // MQTT-3.1.2-20, MQTT-3.1.2-21
		return nil
	}

	var err error
	pkt.Password, err = readBinary(buf)
	if err != nil {
		return err
	}

	return nil
}

func hasFlag(flags byte, mask int) bool {
	return flags&byte(mask) > 0
}
