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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageQueue_Enqueue(t *testing.T) {
	mq := messageQueue{}
	require.Zero(t, mq.list.Len())

	msg := message{id: 1}
	mq.enqueue(msg)
	require.Equal(t, 1, mq.list.Len())
}

func TestMessageQueue_Dequeue(t *testing.T) {
	msg1 := message{id: 1}
	mq := messageQueue{}
	mq.enqueue(msg1)

	msg2 := mq.dequeue()
	require.Zero(t, mq.list.Len())
	assert.Equal(t, msg1.id, msg2.id)
}

func TestMessageQueue_Len(t *testing.T) {
	mq := messageQueue{}
	assert.Zero(t, mq.len())

	msg := message{id: 1}
	mq.enqueue(msg)
	assert.Equal(t, 1, mq.len())

	_ = mq.dequeue()
	assert.Zero(t, mq.len())
}
