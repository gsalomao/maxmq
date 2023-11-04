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

package config_test

import (
	"encoding/json"
	"testing"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	conf := config.DefaultConfig
	err := conf.Validate()
	assert.NoError(t, err)
}

func TestConfigValidateError(t *testing.T) {
	testCases := []struct {
		err    string
		config string
		value  any
	}{
		{"log_level is required", "log_level", ""},
		{"log_level is invalid", "log_level", "invalid"},
		{"log_format is required", "log_format", ""},
		{"log_format is invalid", "log_format", "invalid"},
		{"log_destination is required", "log_destination", ""},
		{"log_destination is invalid", "log_destination", "invalid"},
		{"machine_id must be no greater than 1023", "machine_id", 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.err, func(t *testing.T) {
			js, err := json.Marshal(map[string]any{tc.config: tc.value})
			require.NoError(t, err)

			conf := config.DefaultConfig
			err = json.Unmarshal(js, &conf)
			require.NoError(t, err)

			err = conf.Validate()
			assert.ErrorContains(t, err, tc.err)
		})
	}
}

func TestConfigReadConfigFileNotFound(t *testing.T) {
	err := config.ReadConfigFile()
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, config.ErrConfigFileNotFound)
}

func TestConfigLoadConfig(t *testing.T) {
	conf := config.DefaultConfig

	err := config.LoadConfig(&conf)
	assert.Nil(t, err)
	assert.Equal(t, "info", conf.LogLevel)
	assert.Equal(t, "pretty", conf.LogFormat)
}
