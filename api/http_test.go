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

package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/api"
	"github.com/gsalomao/maxmq/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_NewHTTPServer(t *testing.T) {
	t.Run("MissingLogger", func(t *testing.T) {
		_, err := api.NewHTTPServer(api.Configuration{}, nil)
		assert.NotNil(t, err)
	})

	t.Run("MissingAddress", func(t *testing.T) {
		logStub := mocks.NewLoggerStub()
		_, err := api.NewHTTPServer(api.Configuration{}, logStub.Logger())
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "missing address")
	})
}

func TestAPI_RunInvalidTCPAddress(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	conf := api.Configuration{Address: ":1"}

	srv, err := api.NewHTTPServer(conf, logStub.Logger())
	require.Nil(t, err)
	require.NotNil(t, srv)

	err = srv.Run()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "bind: permission denied")
}

func TestAPI_RunAndStop(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	srv, err := api.NewHTTPServer(api.Configuration{Address: ":8080"},
		logStub.Logger())
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = srv.Run()
		done <- true
	}()

	<-time.After(5 * time.Millisecond)
	assert.Contains(t, logStub.String(), "Listening on [::]:8080")
	srv.Stop()

	<-done
	assert.Nil(t, err)
	assert.Contains(t, logStub.String(), "stopped with success")
}

func TestAPI_GetNotFound(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	srv, err := api.NewHTTPServer(api.Configuration{Address: ":8080"},
		logStub.Logger())
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = srv.Run()
		done <- true
	}()

	<-time.After(5 * time.Millisecond)
	resp, err := http.Get("http://localhost:8080/invalid")
	require.Nil(t, err)
	defer func() { _ = resp.Body.Close() }()

	var httpErr struct{ Message string }
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&httpErr)
	require.Nil(t, err)

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "Not Found", httpErr.Message)
	assert.Contains(t, logStub.String(), "HTTP Request error: Not Found")
	assert.Contains(t, logStub.String(), "HTTP Request received")

	srv.Stop()
	<-done
}

func TestAPI_HeadNotFound(t *testing.T) {
	logStub := mocks.NewLoggerStub()
	srv, err := api.NewHTTPServer(api.Configuration{Address: ":8080"},
		logStub.Logger())
	require.Nil(t, err)

	done := make(chan bool)
	go func() {
		err = srv.Run()
		done <- true
	}()

	<-time.After(5 * time.Millisecond)
	resp, err := http.Head("http://localhost:8080/invalid")
	require.Nil(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, int64(-1), resp.ContentLength)
	assert.Contains(t, logStub.String(), "HTTP Request error: Not Found")
	assert.Contains(t, logStub.String(), "HTTP Request received")

	srv.Stop()
	<-done
}
