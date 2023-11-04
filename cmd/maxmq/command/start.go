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

package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/snowflake"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// NewStart returns a command to start the server.
func NewStart() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start server",
		Long:  "Start the MaxMQ server.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return startServer(cmd.Context(), cmd.Flags())
		},
	}
}

func startServer(ctx context.Context, flags *pflag.FlagSet) error {
	conf := config.DefaultConfig

	confFileFound, err := loadConfig(&conf)
	if err != nil {
		return err
	}

	ctx = logger.Context(ctx, logger.Int("machine_id", conf.MachineID))
	log := newLogger(&conf, flags)

	if confFileFound {
		log.Info(ctx, "Config file loaded with success")
	} else {
		log.Info(ctx, "No config file found")
	}

	config.Watch(func() {
		c := conf
		log.Info(ctx, "Config file changed")

		if found, err := loadConfig(&c); !found {
			log.Warn(ctx, "Config file not found")
		} else if err != nil {
			log.Warn(ctx, "Failed to load config file", logger.Err(err))
			return
		}

		if c.LogLevel != conf.LogLevel {
			lvl, _ := logger.ParseLevel(c.LogLevel)
			log.SetLevel(lvl)
			log.Debug(ctx, "Log level updated",
				logger.Str("old", conf.LogLevel), logger.Str("new", c.LogLevel),
			)
			conf.LogLevel = c.LogLevel
		}
		if c.LogFormat != conf.LogFormat {
			f, _ := logger.ParseFormat(c.LogFormat)
			log.SetFormat(f)
			log.Debug(ctx, "Log format updated",
				logger.Str("old", conf.LogFormat), logger.Str("new", c.LogFormat),
			)
			conf.LogFormat = c.LogFormat
		}
		if c.LogDestination != conf.LogDestination {
			log.Warn(ctx, "Log destination cannot be changed at runtime")
		}
		if c.MachineID != conf.MachineID {
			log.Warn(ctx, "Machine ID cannot be changed at runtime")
		}
	})

	log.Debug(ctx, "Starting server")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	<-sigs
	fmt.Println()

	log.Debug(ctx, "Stopping server")
	return nil
}

func loadConfig(conf *config.Config) (bool, error) {
	var found bool

	err := config.ReadConfigFile()
	if err == nil {
		found = true
	} else if !errors.Is(err, config.ErrConfigFileNotFound) {
		return false, fmt.Errorf("failed to read config file: %w", err)
	}

	err = config.LoadConfig(conf)
	if err != nil {
		return found, fmt.Errorf("failed to load config file: %w", err)
	}

	err = conf.Validate()
	if err != nil {
		return found, fmt.Errorf("invalid config file: %w", err)
	}

	return found, nil
}

func newLogger(conf *config.Config, flags *pflag.FlagSet) *logger.Logger {
	logLevel, _ := logger.ParseLevel(conf.LogLevel)
	logFormat, _ := logger.ParseFormat(conf.LogFormat)
	logDestination, _ := logger.ParseDestination(conf.LogDestination)

	if debug, _ := flags.GetBool("debug"); debug {
		logLevel = logger.LevelDebug
	}

	return logger.New(logDestination, &logger.Options{
		Level:          logLevel,
		Format:         logFormat,
		LogIDGenerator: snowflake.New(conf.MachineID),
	})
}
