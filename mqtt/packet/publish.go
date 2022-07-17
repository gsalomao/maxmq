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
	"encoding/binary"
	"errors"
	"io"
	"time"
)

const (
	publishFlagRetain byte = 0x01
	publishFlagQoS    byte = 0x06
	publishFlagDup    byte = 0x08
)

// Publish represents the PUBLISH Packet from MQTT specifications.
type Publish struct {
	// TopicName identifies the information channel to which Payload data is
	// published.
	TopicName string

	// Payload represents the message payload.
	Payload []byte

	// Properties represents the properties of PUBLISH packet (MQTT V5.0 only).
	Properties *Properties

	// timestamp represents the timestamp which the packet was created.
	timestamp time.Time

	// size represents the number of bytes in the packet.
	size int

	// remainLength represents the number of bytes in the packet excluding the
	// fixed header.
	remainLength int

	// PacketID represents the packet identifier.
	PacketID ID

	// QoS indicates the level of assurance for delivery of the message.
	QoS QoS

	// Version represents the MQTT version.
	Version MQTTVersion

	// Dup indicates that this is the first occasion that the client or broker
	// has attempted to send this packet.
	Dup uint8

	// Retain indicates whether the broker must replace any existing retained
	// message for this topic and store the message, or not.
	Retain uint8
}

func newPacketPublish(opts options) (Packet, error) {
	if opts.packetType != PUBLISH {
		return nil, errors.New("packet type is not PUBLISH")
	}
	if opts.version < MQTT31 || opts.version > MQTT50 {
		return nil, errors.New("invalid version (PUBLISH)")
	}

	qos := QoS(opts.controlFlags & publishFlagQoS >> 1)
	if qos > QoS2 {
		return nil, errors.New("invalid QoS (PUBLISH)")
	}

	dup := opts.controlFlags & publishFlagDup >> 3
	if qos == QoS0 && dup != 0 {
		return nil, errors.New("invalid DUP (PUBLISH)")
	}

	retain := opts.controlFlags & publishFlagRetain
	return &Publish{
		QoS:          qos,
		Dup:          dup,
		Retain:       retain,
		Version:      opts.version,
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the PUBLISH Packet in this broker.
func (pkt *Publish) Pack(w *bufio.Writer) error {
	buf := &bytes.Buffer{}

	if pkt.Version == MQTT50 {
		err := writeProperties(buf, pkt.Properties, PUBLISH)
		if err != nil {
			return err
		}
	}

	// +2 for topic name
	pktLen := buf.Len() + len(pkt.TopicName) + len(pkt.Payload) + 2

	if pkt.QoS > QoS0 {
		pktLen += 2 // +2 for packet ID
	}

	ctrl := (byte(PUBLISH) << packetTypeBit) | (pkt.Dup & 1 << 3) |
		(byte(pkt.QoS) & 3 << 1) | (pkt.Retain & 1)

	_ = w.WriteByte(ctrl)
	_ = writeVarInteger(w, pktLen)
	_, _ = writeBinary(w, []byte(pkt.TopicName))

	if pkt.QoS > QoS0 {
		_ = binary.Write(w, binary.BigEndian, uint16(pkt.PacketID))
	}

	_, _ = buf.WriteTo(w)
	_, _ = w.Write(pkt.Payload)
	return nil
}

// Unpack reads the packet bytes from bufio.Reader and decodes them into the
// packet.
func (pkt *Publish) Unpack(r *bufio.Reader) error {
	msg := make([]byte, pkt.remainLength)
	if _, err := io.ReadFull(r, msg); err != nil {
		return errors.New("missing data")
	}
	buf := bytes.NewBuffer(msg)

	topic, err := readString(buf, pkt.Version)
	if err != nil {
		return err
	}

	topicName := string(topic)
	if !isValidTopicName(topicName) {
		return errors.New("invalid topic name")
	}
	pkt.TopicName = topicName

	if pkt.QoS > 0 {
		var id uint16
		id, err = readUint[uint16](buf, pkt.Version)
		if err != nil {
			return err
		}
		pkt.PacketID = ID(id)
	}

	if pkt.Version == MQTT50 {
		pkt.Properties, _ = readProperties(buf, PUBLISH)
	}

	pkt.Payload = buf.Next(buf.Len())
	return nil
}

// Type returns the packet type.
func (pkt *Publish) Type() Type {
	return PUBLISH
}

// Size returns the packet size in bytes.
func (pkt *Publish) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment which the packet has been
// received.
func (pkt *Publish) Timestamp() time.Time {
	return pkt.timestamp
}