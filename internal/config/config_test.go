// Copyright 2022-2023 The MaxMQ Authors
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
	"testing"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestConfigReadConfigFile(t *testing.T) {
	err := config.ReadConfigFile()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Config File \"maxmq.conf\" Not Found")
}

func TestConfigLoadConfig(t *testing.T) {
	conf, err := config.LoadConfig()
	assert.Nil(t, err)
	assert.Equal(t, "info", conf.LogLevel)
	assert.True(t, conf.MetricsEnabled)
	assert.Equal(t, ":8888", conf.MetricsAddress)
	assert.Equal(t, "/metrics", conf.MetricsPath)
	assert.False(t, conf.MetricsProfiling)
	assert.Equal(t, ":1883", conf.MQTTTCPAddress)
	assert.Equal(t, 1024, conf.MQTTBufferSize)
	assert.Equal(t, 7200, conf.MQTTMaxSessionExpiryInterval)
	assert.Equal(t, 86400, conf.MQTTMaxMessageExpiryInterval)
	assert.Equal(t, 1, conf.MQTTSysTopicUpdateInterval)
	assert.Equal(t, 65535, conf.MQTTMaxTopicAlias)
	assert.Equal(t, 8192, conf.MQTTMaxOutboundMessages)
	assert.Equal(t, 1024, conf.MQTTReceiveMaximum)
	assert.Equal(t, byte(2), conf.MQTTMaximumQoS)
	assert.Equal(t, true, conf.MQTTRetainAvailable)
	assert.Equal(t, true, conf.MQTTWildcardSubscriptionAvailable)
	assert.Equal(t, true, conf.MQTTSubscriptionIDAvailable)
	assert.Equal(t, true, conf.MQTTSharedSubscriptionAvailable)
	assert.Equal(t, byte(3), conf.MQTTMinProtocolVersion)
}
