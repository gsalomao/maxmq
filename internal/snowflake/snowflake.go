// Copyright 2022-2023 The MaxMQ Authors
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

package snowflake

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	// Epoch indicates the unix epoch time for the current implementation in milliseconds.
	Epoch = 1577836800000 // 2020-01-01 00:00:00.000

	// InvalidID indicates the invalid ID.
	InvalidID = 0
)

const (
	timestampBits  = 42
	machineBits    = 10
	sequenceBits   = 12
	timestampShift = machineBits + sequenceBits
	machineShift   = sequenceBits
	machineIDMask  = ^(-1 << machineBits)
	timestampMask  = ^(-1 << timestampBits)
	sequenceMask   = ^(-1 << sequenceBits)
	maxNextIDTries = 3
)

// Timestamp returns the timestamp field (Unix timestamp) in milliseconds of the given ID relative
// to Epoch.
func Timestamp(id uint64) uint64 {
	return id >> timestampShift & timestampMask
}

// TimestampABS returns the absolute timestamp field (Unix timestamp) in milliseconds.
func TimestampABS(id uint64) uint64 {
	return Epoch + Timestamp(id)
}

// MachineID returns the machine identifier field in the given ID.
func MachineID(id uint64) int {
	return int(id >> machineShift & machineIDMask)
}

// Sequence returns the sequence number field in the given ID.
func Sequence(id uint64) uint64 {
	return id & sequenceMask
}

// Snowflake implements the Twitter Snowflake ID generator.
type Snowflake struct {
	machine uint64
	lastID  uint64
}

// New creates an instance of the Snowflake.
func New(machineID int) (*Snowflake, error) {
	if machineID < 0 || machineID > machineIDMask {
		return nil, fmt.Errorf("invalid machineID (must be 0 ≤ id ≤ %d)",
			machineIDMask)
	}

	return &Snowflake{
		machine: uint64(machineID << machineShift),
	}, nil
}

// MachineID returns the machine identifier for the Snowflake instance.
func (s *Snowflake) MachineID() int {
	return int(s.machine >> machineShift)
}

// NextID creates a new ID following the Twitter Snowflake specification.
func (s *Snowflake) NextID() uint64 {
	var nextID uint64

	for i := 0; i < maxNextIDTries; i++ {
		lastID := atomic.LoadUint64(&s.lastID)
		lastTimestamp := Timestamp(lastID)

		now := s.now()
		if now > lastTimestamp {
			nextID = now << timestampShift
		} else {
			nextID = lastID + 1
		}

		nextID |= s.machine
		if atomic.CompareAndSwapUint64(&s.lastID, lastID, nextID) {
			return nextID
		}
	}

	return InvalidID
}

func (s *Snowflake) now() uint64 {
	return uint64(time.Now().UnixMilli()-Epoch) & timestampMask
}
