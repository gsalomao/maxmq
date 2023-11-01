// Copyright 2023 The MaxMQ Authors
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

package snowflake_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/snowflake"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeNew(t *testing.T) {
	testCases := []int{0, 256, 512, 1023}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			sf := snowflake.New(tc)
			assert.Equal(t, tc, sf.MachineID())
		})
	}
}

func TestSnowflakeNextID(t *testing.T) {
	sf := snowflake.New(125)

	id := sf.NextID()
	assert.NotZero(t, id)

	timestamp := snowflake.TimestampABS(id)
	assert.NotZero(t, timestamp)

	now := uint64(time.Now().UnixMilli())
	assert.LessOrEqual(t, timestamp, now)
	assert.Greater(t, timestamp, now-2)

	machineID := snowflake.MachineID(id)
	assert.Equal(t, sf.MachineID(), machineID)

	sequence := snowflake.Sequence(id)
	assert.Zero(t, sequence)
}

func TestSnowflakeNextIDDifferentSequence(t *testing.T) {
	sf := snowflake.New(125)

	id1 := sf.NextID()
	id2 := sf.NextID()
	assert.NotEqual(t, id1, id2)
	assert.Equal(t, snowflake.Timestamp(id1), snowflake.Timestamp(id2))
	assert.Equal(t, snowflake.MachineID(id1), snowflake.MachineID(id2))
	assert.Greater(t, snowflake.Sequence(id2), snowflake.Sequence(id1))
}

func BenchmarkSnowflakeNextID(b *testing.B) {
	b.ReportAllocs()
	sf := snowflake.New(1)

	for i := 0; i < b.N; i++ {
		id := sf.NextID()
		if id == snowflake.InvalidID {
			b.Error("Invalid ID")
		}
	}
}
