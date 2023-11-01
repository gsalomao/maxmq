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

package cli_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/gsalomao/maxmq/internal/cli"
	"github.com/stretchr/testify/assert"
)

func TestCLIRun(t *testing.T) {
	testCases := []struct {
		name string
		cmd  string
		out  string
	}{
		{"Usage", "", "MaxMQ is a Cloud-Native and High-Performance MQTT Broker for IoT"},
		{"ShortVersion", "--version", "MaxMQ OSS 0.0.0"},
	}

	for _, tc := range testCases {
		t.Run(tc.cmd, func(t *testing.T) {
			out := bytes.NewBufferString("")
			c := cli.New("MaxMQ",
				"MaxMQ is a Cloud-Native and High-Performance MQTT Broker for IoT")

			err := c.Run(context.Background(), out, []string{tc.cmd})
			assert.Nil(t, err)
			assert.Contains(t, out.String(), tc.out)
		})
	}
}
