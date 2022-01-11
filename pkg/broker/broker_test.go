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

package broker_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/gsalomao/maxmq/pkg/broker"
	"github.com/gsalomao/maxmq/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroker_Start(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out)
	ctx := context.Background()

	b, err := broker.New(ctx, &log)
	require.Nil(t, err)

	err = b.Start()
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "Starting MaxMQ broker")
}
