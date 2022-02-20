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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketWriter_WritePacket(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := NewWriter(buf, 1024)

	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
	err := wr.WritePacket(&pkt)
	require.Nil(t, err)

	msg := []byte{0x20, 2, 0, 0}
	assert.NotEmpty(t, buf)
	assert.Equal(t, msg, buf.Bytes())
}
