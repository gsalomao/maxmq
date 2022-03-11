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

package cli_test

import (
	"bytes"
	"testing"

	"github.com/gsalomao/maxmq/cli"
	"github.com/stretchr/testify/assert"
)

func TestCLI_Run(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  string
	}{
		{
			name: "Usage",
			in:   "",
			out:  "MaxMQ is a Cloud-Native Message Broker for IoT",
		},
		{
			name: "Version",
			in:   "--version",
			out:  "MaxMQ version",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := bytes.NewBufferString("")
			c := cli.New(out, []string{test.in})

			err := c.Run()

			assert.Nil(t, err)
			assert.Contains(t, out.String(), test.out)
		})
	}
}
