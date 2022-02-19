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
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingResp_Pack(t *testing.T) {
	pkt := PingResp{}
	require.Equal(t, PINGRESP, pkt.Type())

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0xD0, 0}
	assert.Equal(t, msg, buf.Bytes())
}

func BenchmarkPingResp_Pack(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()
		pkt := PingResp{}

		err := pkt.Pack(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPingResp_UnpackUnsupported(t *testing.T) {
	pkt := PingResp{}
	buf := &bytes.Buffer{}
	err := pkt.Unpack(buf)
	require.NotNil(t, err)
}
