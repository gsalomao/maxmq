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
	// Minimal severity level of the logs. (default: info)
	LogLevel string `mapstructure:"log_level"`
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
func LoadConfig() (c Config, err error) {
	viper.AutomaticEnv()

	// Set the default values
	c = Config{
		LogLevel: "info",
	}

	err = viper.Unmarshal(&c)
	return
}
