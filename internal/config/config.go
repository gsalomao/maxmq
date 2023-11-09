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

package config

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/fsnotify/fsnotify"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
	"github.com/spf13/viper"
)

// ErrConfigFileNotFound indicates that the configuration file was not found.
var ErrConfigFileNotFound = errors.New("config file not found")

// DefaultConfig contains the default configuration.
var DefaultConfig = Config{
	LogLevel:       "info",
	LogFormat:      "pretty",
	LogDestination: "stdout",
	MachineID:      0,
	MetricsEnabled: true,
	MetricsHost:    "localhost",
	MetricsPort:    8888,
	MetricsPath:    "/metrics",
}

func init() {
	viper.SetConfigName("maxmq.yaml")
	viper.SetConfigType("yaml")

	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.maxmq")
	viper.AddConfigPath("/etc/maxmq")
}

// Config holds all the application configuration.
type Config struct {
	// LogLevel sets the minimal severity level to log.
	LogLevel string `json:"log_level" mapstructure:"log_level"`

	// LogFormat sets the log format.
	LogFormat string `json:"log_format" mapstructure:"log_format"`

	// LogDestination sets the log destination.
	LogDestination string `json:"log_destination" mapstructure:"log_destination"`

	// MachineID sets the machine identifier.
	MachineID int `json:"machine_id" mapstructure:"machine_id"`

	// MetricsEnabled indicates whether the server exports metrics or not.
	MetricsEnabled bool `json:"metrics_enabled" mapstructure:"metrics_enabled"`

	// MetricsHost indicates the host address where the Prometheus metrics are exported.
	MetricsHost string `json:"metrics_host" mapstructure:"metrics_host"`

	// MetricsPort indicates the port where the Prometheus metrics are exported.
	MetricsPort int `json:"metrics_port" mapstructure:"metrics_port"`

	// MetricsPath indicates the path where the metrics are exported.
	MetricsPath string `json:"metrics_path" mapstructure:"metrics_path"`

	// MetricsProfiling indicates whether the profiling metrics are exported or not.
	MetricsProfiling bool `json:"metrics_profiling" mapstructure:"metrics_profiling"`
}

func (c *Config) Validate() error {
	err := validation.ValidateStruct(c,
		validation.Field(&c.LogLevel,
			validation.Required.Error(errorMessage("required")),
			validation.In("debug", "Debug", "DEBUG", "info", "Info", "INFO",
				"warn", "Warn", "WARN", "error", "Error", "ERROR").
				Error(errorMessage("invalid")),
		),
		validation.Field(&c.LogFormat,
			validation.Required.Error(errorMessage("required")),
			validation.In("json", "Json", "JSON", "text", "Text", "TEXT", "pretty", "Pretty",
				"PRETTY", "pretty-no-colors", "Pretty-No-Colors", "PRETTY-NO-COLORS").
				Error(errorMessage("invalid")),
		),
		validation.Field(&c.LogDestination,
			validation.Required.Error(errorMessage("required")),
			validation.In("stdout", "Stdout", "STDOUT", "stderr", "Stderr", "STDERR").
				Error(errorMessage("invalid")),
		),
		validation.Field(&c.MachineID, validation.Max(1023)),
		validation.Field(&c.MetricsHost, is.Host),
		validation.Field(&c.MetricsPort, validation.Min(1024), validation.Max(65535)),
		validation.Field(&c.MetricsPath, validation.Match(regexp.MustCompile(`^/\w+`))),
	)

	var vErr validation.Errors
	if !errors.As(err, &vErr) {
		return err
	}

	for f, e := range vErr {
		return fmt.Errorf("%s %s", f, e.Error())
	}

	return nil
}

// ReadConfigFile reads the configuration file in YAML format.
func ReadConfigFile() error {
	if err := viper.ReadInConfig(); err != nil {
		var vErr viper.ConfigFileNotFoundError
		if errors.As(err, &vErr) {
			return ErrConfigFileNotFound
		}
		return err
	}

	return nil
}

// LoadConfig loads the configuration from the conf file, environment variables,
// or use the default values.
//
// Note: The ReadConfigFile must be called before in order to load the
// configuration from the conf file.
func LoadConfig(c *Config) error {
	viper.SetEnvPrefix("MAXMQ")
	viper.AutomaticEnv()
	bindEnvs(*c)

	return viper.Unmarshal(c)
}

// Watch watches for changes in the config file.
func Watch(onChange func()) {
	if onChange != nil {
		viper.OnConfigChange(func(_ fsnotify.Event) {
			onChange()
		})
	}
	viper.WatchConfig()
}

var errorMessages = map[string]string{
	"required": "is required",
	"invalid":  "is invalid",
}

func errorMessage(code string) string {
	str, ok := errorMessages[code]
	if !ok {
		panic(fmt.Sprintf("invalid error message code: %s", code))
	}
	return str
}

func bindEnvs(conf any, parts ...string) {
	ifv := reflect.ValueOf(conf)
	ift := reflect.TypeOf(conf)
	for i := 0; i < ift.NumField(); i++ {
		v := ifv.Field(i)
		t := ift.Field(i)
		tv, ok := t.Tag.Lookup("mapstructure")
		if !ok {
			continue
		}
		switch v.Kind() {
		case reflect.Struct:
			bindEnvs(v.Interface(), append(parts, tv)...)
		default:
			_ = viper.BindEnv(strings.Join(append(parts, tv), "."))
		}
	}
}
