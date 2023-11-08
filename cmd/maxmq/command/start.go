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
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/metric"
	"github.com/gsalomao/maxmq/internal/snowflake"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// NewStart returns a command to start the server.
func NewStart() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start server",
		Long:  "Start the MaxMQ server.",
		Run: func(cmd *cobra.Command, _ []string) {
			startServer(cmd.Context(), cmd.Flags())
		},
	}

	flags := cmd.PersistentFlags()
	flags.Bool("debug", false, "Enable debug logs")
	flags.Bool("profile", false, "Enable profiling")

	return cmd
}

func startServer(ctx context.Context, flags *pflag.FlagSet) {
	conf := config.DefaultConfig
	profile, _ := flags.GetBool("profile")

	confFileFound, err := loadConfig(&conf)
	if err != nil {
		fmt.Printf("Failed to load config: %s", err.Error())
		os.Exit(1)
	}

	ctx = logger.Context(ctx, logger.Int("machine_id", conf.MachineID))
	log := newLogger(&conf, flags)

	if confFileFound {
		log.Info(ctx, "Config file loaded with success")
	} else {
		log.Info(ctx, "No config file found")
	}

	var cpu *os.File
	if profile {
		cpu, err = os.Create("cpu.prof")
		if err != nil {
			log.Error(ctx, "Failed to create CPU profile file", logger.Err(err))
			os.Exit(1)
		}

		if err = pprof.StartCPUProfile(cpu); err != nil {
			log.Error(ctx, "Failed to start CPU profile", logger.Err(err))
			os.Exit(1)
		}
	}
	defer func() {
		if profile {
			var heap *os.File
			log.Debug(ctx, "Saving CPU and memory profiles")

			heap, err = os.Create("heap.prof")
			if err != nil {
				log.Error(ctx, "Failed to create Heap profile file", logger.Err(err))
				os.Exit(1)
			}

			runtime.GC()
			if err = pprof.WriteHeapProfile(heap); err != nil {
				log.Error(ctx, "Failed to save Heap profile", logger.Err(err))
				os.Exit(1)
			}

			pprof.StopCPUProfile()
			_ = heap.Close()
			_ = cpu.Close()
		}
	}()

	runServer(ctx, log, &conf)
}

func runServer(ctx context.Context, log *logger.Logger, conf *config.Config) {
	var (
		err     error
		metrics *metric.Server
		wg      sync.WaitGroup
	)

	defer func() {
		if metrics != nil {
			log.Debug(ctx, "Closing metrics server")

			err = metrics.Close()
			if err != nil {
				log.Error(ctx, "Failed to close metrics server", logger.Err(err))
			}
		}
	}()

	config.Watch(func() {
		c := *conf
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

	if conf.MetricsEnabled {
		metrics = metric.NewServer(log,
			metric.WithAddress(fmt.Sprintf("%s:%v", conf.MetricsHost, conf.MetricsPort)),
			metric.WithPath(conf.MetricsPath),
			metric.WithProfile(conf.MetricsProfiling),
		)

		serveMetricsServer(ctx, log, metrics, &wg)
	}

	waitSignal()
	log.Debug(ctx, "Stopping server")

	if metrics != nil {
		log.Debug(ctx, "Shutting down metrics server")

		// TODO: Create a new context for graceful shutdown.
		if err = metrics.Shutdown(ctx); err != nil {
			log.Error(ctx, "Failed to shutdown metrics server", logger.Err(err))
			return
		}
	}

	wg.Wait()
}

func serveMetricsServer(ctx context.Context, log *logger.Logger, metrics *metric.Server, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := metrics.Serve(ctx)
		if err != nil {
			log.Error(ctx, "Failed to run metrics server", logger.Err(err))
		}
	}()
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

func waitSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	<-sigs
	fmt.Println()
}
