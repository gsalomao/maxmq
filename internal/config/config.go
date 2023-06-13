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

package config

import (
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config holds all the application configuration.
type Config struct {
	// The minimal severity level to log.
	LogLevel string `mapstructure:"log_level"`

	// The log format.
	LogFormat string `mapstructure:"log_format"`

	// The ID of the machine.
	MachineID int `mapstructure:"machine_id"`

	// Indicate whether the server exports metrics or not.
	MetricsEnabled bool `mapstructure:"metrics_enabled"`

	// TCP address (<IP>:<port>) where the Prometheus metrics are exported.
	MetricsAddress string `mapstructure:"metrics_address"`

	// The path where the metrics are exported.
	MetricsPath string `mapstructure:"metrics_path"`

	// Indicate whether the profiling metrics are exported or not.
	MetricsProfiling bool `mapstructure:"metrics_profiling"`

	// TCP address (<IP>:<port>) that the MQTT will bind to.
	MQTTTCPAddress string `mapstructure:"mqtt_tcp_address"`

	// The number of seconds to wait for the MQTT server to shut down gracefully.
	MQTTShutdownTimeout int `mapstructure:"mqtt_shutdown_timeout"`

	// The size, in bytes, of the MQTT receiver and transmitter buffers.
	MQTTBufferSize int `mapstructure:"mqtt_buffer_size"`

	// The maximum size, in bytes, allowed for MQTT Packets.
	MQTTMaxPacketSize int `mapstructure:"mqtt_max_packet_size"`

	// The maximum period, in seconds, a MQTT session is still valid after the network connection with the client has
	// been closed.
	MQTTMaxSessionExpiryInterval int `mapstructure:"mqtt_max_session_expiry_interval"`

	// The maximum period, in seconds, a MQTT message is still valid.
	MQTTMaxMessageExpiryInterval int `mapstructure:"mqtt_max_message_expiry_interval"`

	// The interval, in seconds, to send MQTT messages in the $SYS topic.
	MQTTSysTopicUpdateInterval int `mapstructure:"mqtt_sys_topic_update_interval"`

	// The maximum number of topic aliases that an MQTT V5 client is allowed to create.
	MQTTMaxTopicAlias int `mapstructure:"mqtt_max_topic_alias"`

	// The maximum number of outbound message for a client.
	MQTTMaxOutboundMessages int `mapstructure:"mqtt_max_outbound_messages"`

	// The maximum number of QoS 1 and QoS 2 publications that it is willing to process concurrently.
	MQTTReceiveMaximum int `mapstructure:"mqtt_receive_maximum"`

	// The maximum MQTT QoS for PUBLISH Packets accepted by the server.
	MQTTMaximumQoS byte `mapstructure:"mqtt_max_qos"`

	// Indicate whether the server allows retained MQTT messages or not.
	MQTTRetainAvailable bool `mapstructure:"mqtt_retain_available"`

	// Indicate whether the server allows MQTT wildcard subscription or not.
	MQTTWildcardSubscriptionAvailable bool `mapstructure:"mqtt_wildcard_subscription_available"`

	// Indicate whether the server allows MQTT subscription identifier or not.
	MQTTSubscriptionIDAvailable bool `mapstructure:"mqtt_subscription_identifier_available"`

	// Indicate whether the server allows MQTT shared subscription or not.
	MQTTSharedSubscriptionAvailable bool `mapstructure:"mqtt_shared_subscription_available"`

	// Set the minimal MQTT protocol version.
	MQTTMinProtocolVersion byte `mapstructure:"mqtt_min_protocol_version"`
}

// DefaultConfig contains the default configuration.
var DefaultConfig = Config{
	LogLevel:                          "info",
	LogFormat:                         "pretty",
	MetricsEnabled:                    true,
	MetricsAddress:                    ":8888",
	MetricsPath:                       "/metrics",
	MQTTTCPAddress:                    ":1883",
	MQTTShutdownTimeout:               15,
	MQTTBufferSize:                    1024,
	MQTTMaxSessionExpiryInterval:      7200,
	MQTTMaxMessageExpiryInterval:      86400,
	MQTTSysTopicUpdateInterval:        1,
	MQTTMaxTopicAlias:                 65535,
	MQTTMaxOutboundMessages:           8192,
	MQTTReceiveMaximum:                1024,
	MQTTMaximumQoS:                    2,
	MQTTRetainAvailable:               true,
	MQTTWildcardSubscriptionAvailable: true,
	MQTTSubscriptionIDAvailable:       true,
	MQTTSharedSubscriptionAvailable:   true,
	MQTTMinProtocolVersion:            3,
}

// ReadConfigFile reads the configuration file.
//
// The configuration file can be stored at one of the following locations:
//   - /etc/maxmq.conf
//   - /etc/maxmq/maxmq.conf
func ReadConfigFile() error {
	viper.SetConfigName("maxmq.conf")
	viper.SetConfigType("toml")

	if exe, err := os.Executable(); err == nil {
		pwd := filepath.Dir(exe)
		viper.AddConfigPath(pwd)

		root := filepath.Dir(pwd + "/../")
		viper.AddConfigPath(root)
	}

	viper.AddConfigPath("/etc/maxmq")
	viper.AddConfigPath("/etc")

	return viper.ReadInConfig()
}

// LoadConfig loads the configuration from the conf file, environment variables,
// or use the default values.
//
// Note: The ReadConfigFile must be called before in order to load the
// configuration from the conf file.
func LoadConfig() (c Config, err error) {
	viper.SetEnvPrefix("MAXMQ")
	viper.AutomaticEnv()

	// Bind environment variables
	_ = viper.BindEnv("log_level")
	_ = viper.BindEnv("log_format")
	_ = viper.BindEnv("machine_id")
	_ = viper.BindEnv("metrics_enabled")
	_ = viper.BindEnv("metrics_address")
	_ = viper.BindEnv("metrics_path")
	_ = viper.BindEnv("metrics_profiling")
	_ = viper.BindEnv("mqtt_tcp_address")
	_ = viper.BindEnv("mqtt_shutdown_timeout")
	_ = viper.BindEnv("mqtt_buffer_size")
	_ = viper.BindEnv("mqtt_max_packet_size")
	_ = viper.BindEnv("mqtt_max_session_expiry_interval")
	_ = viper.BindEnv("mqtt_max_message_expiry_interval")
	_ = viper.BindEnv("mqtt_sys_topic_update_interval")
	_ = viper.BindEnv("mqtt_max_topic_alias")
	_ = viper.BindEnv("mqtt_max_outbound_messages")
	_ = viper.BindEnv("mqtt_receive_maximum")
	_ = viper.BindEnv("mqtt_max_qos")
	_ = viper.BindEnv("mqtt_retain_available")
	_ = viper.BindEnv("mqtt_wildcard_subscription_available")
	_ = viper.BindEnv("mqtt_subscription_identifier_available")
	_ = viper.BindEnv("mqtt_shared_subscription_available")
	_ = viper.BindEnv("mqtt_min_protocol_version")

	// Set the default values
	c = DefaultConfig

	err = viper.Unmarshal(&c)
	return c, err
}
