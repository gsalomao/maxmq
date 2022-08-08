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

package mqtt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessageIDGenerator_NextIDValid(t *testing.T) {
	generator := newMessageIDGenerator(10)

	id1 := generator.nextID()
	id2 := generator.nextID()
	assert.NotZero(t, id1)
	assert.NotZero(t, id2)
}

func TestMessageIDGenerator_NextIDTimestamp(t *testing.T) {
	generator := newMessageIDGenerator(10)

	id1 := generator.nextID()
	id2 := generator.nextID()

	timestamp1 := id1 >> 23
	timestamp2 := id2 >> 23
	assert.Equal(t, timestamp1, timestamp2)
	assert.NotZero(t, timestamp1)
}

func TestMessageIDGenerator_NextIDNodeID(t *testing.T) {
	generator := newMessageIDGenerator(10)

	id1 := generator.nextID()
	id2 := generator.nextID()

	nodeID1 := uint16(id1>>14) & 0x3FF
	nodeID2 := uint16(id2>>14) & 0x3FF
	assert.Equal(t, uint16(10), nodeID1)
	assert.Equal(t, uint16(10), nodeID2)
}

func TestMessageIDGenerator_NextIDSequenceNumber(t *testing.T) {
	generator := newMessageIDGenerator(10)

	id1 := generator.nextID()
	id2 := generator.nextID()

	sqn1 := id1 & 0x1FFF
	sqn2 := id2 & 0x1FFF
	assert.Equal(t, sqn1+1, sqn2)
}

func TestMessageIDGenerator_NextIDSameTimestamp(t *testing.T) {
	generator := newMessageIDGenerator(10)

	id1 := generator.nextID()
	id2 := generator.nextID()

	timestamp1 := id1 >> 24
	timestamp2 := id2 >> 24
	assert.Equal(t, timestamp2, timestamp1)

	sqn1 := id1 & 0x1FFF
	sqn2 := id2 & 0x1FFF
	assert.Greater(t, sqn2, sqn1)
}

func TestMessageIDGenerator_NextIDDifferentTimestamp(t *testing.T) {
	generator := newMessageIDGenerator(10)

	id1 := generator.nextID()
	time.Sleep(1 * time.Millisecond)
	id2 := generator.nextID()

	timestamp1 := id1 >> 24
	timestamp2 := id2 >> 24
	assert.Greater(t, timestamp2, timestamp1)

	sqn1 := id1 & 0x1FFF
	sqn2 := id2 & 0x1FFF
	assert.Equal(t, sqn1, sqn2)
}

func BenchmarkMessageIDGenerator_NextID(b *testing.B) {
	b.ReportAllocs()
	generator := newMessageIDGenerator(10)

	for i := 0; i < b.N; i++ {
		_ = generator.nextID()
	}
}
