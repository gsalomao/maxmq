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

package metric_test

import (
	"context"
	"io"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerNewServer(t *testing.T) {
	log := logger.New(io.Discard, nil)

	srv := metric.NewServer(log)
	assert.NotNil(t, srv)

	_ = srv.Close()
}

func TestServerNewServerWithOptions(t *testing.T) {
	log := logger.New(io.Discard, nil)

	srv := metric.NewServer(log,
		metric.WithAddress(":8888"),
		metric.WithPath("/metrics"),
		metric.WithProfile(true),
	)
	assert.NotNil(t, srv)

	_ = srv.Close()
}

func TestServerServe(t *testing.T) {
	log := logger.New(io.Discard, nil)

	srv := metric.NewServer(log)
	require.NotNil(t, srv)
	defer func() { _ = srv.Close() }()

	var err error
	ready := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		close(ready)
		err = srv.Serve(context.Background())
	}()

	<-ready
	require.NoError(t, srv.Shutdown(context.Background()))

	<-done
	require.NoError(t, err)
}

func TestServerServeInvalidAddress(t *testing.T) {
	log := logger.New(io.Discard, nil)

	srv := metric.NewServer(log, metric.WithAddress("."))
	require.NotNil(t, srv)

	err := srv.Serve(context.Background())
	require.Error(t, err)
}
