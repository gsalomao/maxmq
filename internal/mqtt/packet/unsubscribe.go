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

// Unsubscribe represents the UNSUBSCRIBE Packet from MQTT specifications.
type Unsubscribe struct {
	// Topics represents the list of topics to unsubscribe.
	Topics []string

	// Properties represents the SUBSCRIBE properties (MQTT V5.0 only).
	Properties *Properties

	// PacketID represents the packet identifier.
	PacketID ID

	// Version represents the MQTT version.
	Version MQTTVersion

	// Unexported fields
	timestamp    time.Time
	size         int
	remainLength int
}

func newPacketUnsubscribe(opts options) (Packet, error) {
	if opts.packetType != UNSUBSCRIBE {
		return nil, errors.New("packet type is not UNSUBSCRIBE")
	}
	if opts.controlFlags != 2 {
		return nil, errors.New("invalid Control Flags (UNSUBSCRIBE)")
	}
	if opts.version < MQTT31 || opts.version > MQTT50 {
		return nil, errors.New("invalid version (UNSUBSCRIBE)")
	}

	return &Unsubscribe{
		Version:      opts.version,
		Topics:       make([]string, 0),
		size:         opts.fixedHeaderLength + opts.remainingLength,
		remainLength: opts.remainingLength,
		timestamp:    opts.timestamp,
	}, nil
}

// Write encodes the packet into bytes and writes it into the io.Writer.
// It is not supported by the UNSUBSCRIBE Packet.
func (pkt *Unsubscribe) Write(_ *bufio.Writer) error {
	return errors.New("unsupported (UNSUBSCRIBE)")
}

// Read reads the packet bytes from bufio.Reader and decodes them into the
// packet.
func (pkt *Unsubscribe) Read(r *bufio.Reader) error {
	msg := make([]byte, pkt.remainLength)
	if _, err := io.ReadFull(r, msg); err != nil {
		return fmt.Errorf("failed to read remaining bytes: %w",
			ErrV5MalformedPacket)
	}
	buf := bytes.NewBuffer(msg)

	id, err := readUint[uint16](buf)
	pkt.PacketID = ID(id)
	if err != nil {
		return fmt.Errorf("failed to read packet ID: %w", err)
	}

	if pkt.Version == MQTT50 {
		pkt.Properties, err = readProperties(buf, UNSUBSCRIBE)
		if err != nil {
			return fmt.Errorf("failed to read properties: %w", err)
		}
	}

	err = pkt.readTopics(buf)
	if err != nil {
		return fmt.Errorf("failed to read topics: %w", err)
	}

	return nil
}

// Type returns the packet type.
func (pkt *Unsubscribe) Type() Type {
	return UNSUBSCRIBE
}

// Size returns the packet size in bytes.
func (pkt *Unsubscribe) Size() int {
	return pkt.size
}

// Timestamp returns the timestamp of the moment which the packet has been
// received.
func (pkt *Unsubscribe) Timestamp() time.Time {
	return pkt.timestamp
}

func (pkt *Unsubscribe) readTopics(buf *bytes.Buffer) error {
	for {
		topic, err := readString(buf)
		if err != nil {
			return err
		}

		pkt.Topics = append(pkt.Topics, string(topic))
		if buf.Len() == 0 {
			return nil
		}
	}
}
