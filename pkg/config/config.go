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

package config

import (
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config holds all the application configuration.
type Config struct {
	// Minimal severity level of the logs.
	LogLevel string `mapstructure:"log_level"`

	// TCP address (<IP>:<port>) that the MQTT will bind to.
	MQTTTCPAddress string `mapstructure:"mqtt_tcp_address"`

	// The amount of time, in seconds, the MQTT waits for the CONNECT Packet.
	MQTTConnectTimeout int `mapstructure:"mqtt_connect_timeout"`

	// The size, in bytes, of the MQTT receiver and transmitter buffers.
	MQTTBufferSize int `mapstructure:"mqtt_buffer_size"`

	// The maximum size, in bytes, allowed for MQTT Packets.
	MQTTMaxPacketSize int `mapstructure:"mqtt_max_packet_size"`
}

// ReadConfigFile reads the configuration file.
//
// The configuration file can be stored at one of the following locations:
//  - /etc/maxmq.conf
//  - /etc/maxmq/maxmq.conf
func ReadConfigFile() error {
	viper.SetConfigName("maxmq.conf")
	viper.SetConfigType("toml")
	viper.AddConfigPath("/etc")
	viper.AddConfigPath("/etc/maxmq")

	if exe, err := os.Executable(); err == nil {
		root := filepath.Dir(exe) + "/../"
		root = filepath.Dir(root)
		viper.AddConfigPath(root)
	}

	return viper.ReadInConfig()
}

// LoadConfig loads the configuration from the conf file, environment variables,
// or use the default values.
//
// Note: The ReadConfigFile must be called before in order to load the
// configuration from the conf file.
func LoadConfig() (Config, error) {
	viper.SetEnvPrefix("MAXMQ")
	viper.AutomaticEnv()

	// Bind environment variables
	_ = viper.BindEnv("log_level")
	_ = viper.BindEnv("mqtt_tcp_address")
	_ = viper.BindEnv("mqtt_connect_timeout")
	_ = viper.BindEnv("mqtt_buffer_size")
	_ = viper.BindEnv("mqtt_max_packet_size")

	// Set the default values
	c := Config{
		LogLevel:           "info",
		MQTTTCPAddress:     ":1883",
		MQTTConnectTimeout: 5,
		MQTTBufferSize:     1024,
		MQTTMaxPacketSize:  65536,
	}

	err := viper.Unmarshal(&c)
	return c, err
}
