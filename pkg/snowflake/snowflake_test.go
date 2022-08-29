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

package snowflake_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnowflake_MachineIDValid(t *testing.T) {
	tests := []int{0, 256, 512, 1023}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			sf, err := snowflake.New(test)
			assert.Nil(t, err)
			require.NotNil(t, sf)
			assert.Equal(t, test, sf.MachineID())
		})
	}
}

func TestSnowflake_MachineIDInvalid(t *testing.T) {
	tests := []int{-1, 1024}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			sf, err := snowflake.New(test)
			require.NotNil(t, err)
			assert.Nil(t, sf)
		})
	}
}

func TestSnowflake_NextID(t *testing.T) {
	sf, _ := snowflake.New(125)

	id := sf.NextID()
	require.NotZero(t, id)

	timestamp := snowflake.TimestampABS(id)
	require.NotZero(t, timestamp)

	now := uint64(time.Now().UnixMilli())
	assert.LessOrEqual(t, timestamp, now)
	assert.Greater(t, timestamp, now-2)

	machineID := snowflake.MachineID(id)
	assert.Equal(t, sf.MachineID(), machineID)

	sequence := snowflake.Sequence(id)
	assert.Zero(t, sequence)
}

func TestSnowflake_NextIDDifferentSequence(t *testing.T) {
	sf, _ := snowflake.New(125)

	id1 := sf.NextID()
	id2 := sf.NextID()
	require.NotEqual(t, id1, id2)
	assert.Equal(t, snowflake.Timestamp(id1), snowflake.Timestamp(id2))
	assert.Equal(t, snowflake.MachineID(id1), snowflake.MachineID(id2))
	assert.Greater(t, snowflake.Sequence(id2), snowflake.Sequence(id1))
}

func BenchmarkSnowflake_NextID(b *testing.B) {
	b.ReportAllocs()
	sf, _ := snowflake.New(1)

	for i := 0; i < b.N; i++ {
		id := sf.NextID()
		if id == snowflake.InvalidID {
			b.Error("Invalid ID")
		}
	}
}
