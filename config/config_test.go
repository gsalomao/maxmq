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

package config_test

import (
	"testing"

	"github.com/gsalomao/maxmq/config"
	"github.com/stretchr/testify/assert"
)

func TestConfig_ReadConfigFile(t *testing.T) {
	err := config.ReadConfigFile()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Config File \"maxmq.conf\" Not Found")
}

func TestConfig_LoadConfig(t *testing.T) {
	conf, err := config.LoadConfig()
	assert.Nil(t, err)
	assert.Equal(t, "info", conf.LogLevel)
	assert.Equal(t, ":1883", conf.MQTTTCPAddress)
	assert.Equal(t, 5, conf.MQTTConnectTimeout)
	assert.Equal(t, 1024, conf.MQTTBufferSize)
	assert.Equal(t, 0, conf.MQTTMaxKeepAlive)
	assert.Equal(t, uint32(0), conf.MQTTSessionExpiration)
	assert.Equal(t, 2, conf.MQTTMaximumQoS)
	assert.Equal(t, true, conf.MQTTRetainAvailable)
}