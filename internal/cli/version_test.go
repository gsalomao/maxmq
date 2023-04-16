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

package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCLIRunVersion(t *testing.T) {
	out := bytes.NewBufferString("")
	c := New(out, []string{"version"})

	err := c.Run()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Version:")
	assert.Contains(t, out.String(), "Revision:")
	assert.Contains(t, out.String(), "Built:")
	assert.Contains(t, out.String(), "Distribution:")
	assert.Contains(t, out.String(), "Platform:")
	assert.Contains(t, out.String(), "Go version:")
}

func TestCLIRunVersionHelp(t *testing.T) {
	out := bytes.NewBufferString("")
	c := New(out, []string{"version", "--help"})

	err := c.Run()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Show version and build summary")
}
