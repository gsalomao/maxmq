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

package mqtt

import (
	"bytes"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerNewServer(t *testing.T) {
	s, err := NewServer()
	assert.NotNil(t, s)
	assert.Nil(t, err)
}

func TestServerNewServerWithOptions(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	s, err := NewServer(
		WithConfig(&Config{
			SharedSubscriptionAvailable:   true,
			RetainAvailable:               true,
			WildcardSubscriptionAvailable: true,
			SubscriptionIDAvailable:       true,
		}),
		WithLogger(log),
		WithMachineID(0),
	)
	assert.NotNil(t, s)
	assert.Nil(t, err)
}

func TestServerStartAndStop(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	s, err := NewServer(WithLogger(log))
	require.Nil(t, err)

	err = s.Start()
	assert.Nil(t, err)
	s.Stop()
}

func TestServerStartError(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	s, err := NewServer(
		WithConfig(&Config{
			TCPAddress: ":abc",
		}),
		WithLogger(log),
	)
	require.Nil(t, err)

	err = s.Start()
	assert.NotNil(t, err)
}
