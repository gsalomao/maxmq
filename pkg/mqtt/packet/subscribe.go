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
	"io"
	"time"
)

// Subscribe represents the SUBSCRIBE Packet from MQTT specifications.
type Subscribe struct {
	// Topics represents the list of topics to subscribe.
	Topics []Topic

	// Properties represents the SUBSCRIBE properties (MQTT V5.0 only).
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

	// Version represents the MQTT version.
	Version MQTTVersion
}

func newPacketSubscribe(opts options) (Packet, error) {
	if opts.packetType != SUBSCRIBE {
		return nil, errors.New("packet type is not SUBSCRIBE")
	}
	if opts.controlFlags != 2 {
		return nil, errors.New("invalid Control Flags (SUBSCRIBE)")
	}
	if opts.version < MQTT31 || opts.version > MQTT50 {
		return nil, errors.New("invalid version (SUBSCRIBE)")
	}

	return &Subscribe{
		Version:      opts.version,
		Topics:       make([]Topic, 0),
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// Pack encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the SUBSCRIBE Packet in this broker.
func (pkt *Subscribe) Pack(_ *bufio.Writer) error {
	return errors.New("unsupported (SUBSCRIBE)")
}

// Unpack reads the packet bytes from bufio.Reader and decodes them into the
// packet.
func (pkt *Subscribe) Unpack(r *bufio.Reader) error {
	msg := make([]byte, pkt.remainLength)
	if _, err := io.ReadFull(r, msg); err != nil {
		return errors.New("missing data")
	}
	buf := bytes.NewBuffer(msg)

	id, err := readUint[uint16](buf, pkt.Version)
	if err != nil {
		return err
	}
	pkt.PacketID = ID(id)

	if pkt.Version == MQTT50 {
		pkt.Properties, err = readProperties(buf, SUBSCRIBE)
		if err != nil {
			return err
		}
	}

	err = pkt.unpackTopics(buf)
	if err != nil {
		return err
	}

	return nil
}

// Type returns the packet type.
func (pkt *Subscribe) Type() Type {
	return SUBSCRIBE
}

// Size returns the packet size in bytes.
func (pkt *Subscribe) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment which the packet has been
// received.
func (pkt *Subscribe) Timestamp() time.Time {
	return pkt.timestamp
}

func (pkt *Subscribe) unpackTopics(buf *bytes.Buffer) error {
	for {
		topic, err := readString(buf, pkt.Version)
		if err != nil {
			return err
		}

		opts, err := buf.ReadByte()
		if err != nil {
			return err
		}

		optsMask := byte(0xFC)
		if pkt.Version == MQTT50 {
			optsMask = 0xC0
		}
		if opts&optsMask > 0 {
			return errors.New("invalid subscription options")
		}

		if opts&0x03 > 2 {
			return errors.New("invalid QoS")
		}

		if (opts & 0x30 >> 4) > 2 {
			return errors.New("invalid Retain Handling")
		}

		pkt.Topics = append(pkt.Topics, Topic{
			Name:              string(topic),
			QoS:               QoS(opts & 0x03),
			NoLocal:           (opts & 0x04 >> 2) > 0,
			RetainAsPublished: (opts & 0x08 >> 3) > 0,
			RetainHandling:    opts & 0x20 >> 4,
		})

		if buf.Len() == 0 {
			return nil
		}
	}
}
