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
	"time"

	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/listener"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/metric"
	"github.com/gsalomao/maxmq/internal/mqtt"
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
			app := application{conf: config.DefaultConfig}

			confFileFound, err := loadConfig(&app.conf)
			if err != nil {
				fmt.Printf("Failed to load config: %s", err.Error())
				os.Exit(1)
			}

			flags := cmd.Flags()
			ctx := logger.Context(cmd.Context(), logger.Int("machine_id", app.conf.MachineID))

			log := newLogger(&app.conf, flags)
			app.log = log
			app.profile, _ = flags.GetBool("profile")

			if confFileFound {
				log.Info(ctx, "Config file loaded with success")
			} else {
				log.Info(ctx, "No config file found")
			}

			app.watchConfig(ctx)

			log.Debug(ctx, "Starting server")
			if err = app.run(ctx); err != nil {
				log.Error(ctx, "Failed to run server", logger.Err(err))
				os.Exit(1)
			}

			log.Info(ctx, "Server stopped with success")
		},
	}

	flags := cmd.PersistentFlags()
	flags.Bool("debug", false, "Enable debug logs")
	flags.Bool("profile", false, "Enable profiling")

	return cmd
}

type application struct {
	log     *logger.Logger
	metrics *metric.Server
	mqtt    *mqtt.Server
	conf    config.Config
	wg      sync.WaitGroup
	mu      sync.RWMutex
	profile bool
}

func (a *application) watchConfig(ctx context.Context) {
	config.Watch(func() {
		a.mu.Lock()
		defer a.mu.Unlock()

		c := a.conf
		a.log.Info(ctx, "Config file changed")

		if found, err := loadConfig(&c); !found {
			a.log.Warn(ctx, "Config file not found")
		} else if err != nil {
			a.log.Warn(ctx, "Failed to load config file", logger.Err(err))
			return
		}

		if c.LogLevel != a.conf.LogLevel {
			lvl, _ := logger.ParseLevel(c.LogLevel)
			a.log.SetLevel(lvl)
			a.log.Debug(ctx, "Log level updated",
				logger.Str("old", a.conf.LogLevel), logger.Str("new", c.LogLevel),
			)
			a.conf.LogLevel = c.LogLevel
		}
		if c.LogFormat != a.conf.LogFormat {
			f, _ := logger.ParseFormat(c.LogFormat)
			a.log.SetFormat(f)
			a.log.Debug(ctx, "Log format updated",
				logger.Str("old", a.conf.LogFormat), logger.Str("new", c.LogFormat),
			)
			a.conf.LogFormat = c.LogFormat
		}
		if c.LogDestination != a.conf.LogDestination {
			a.log.Warn(ctx, "Log destination cannot be changed at runtime")
		}
		if c.MachineID != a.conf.MachineID {
			a.log.Warn(ctx, "Machine ID cannot be changed at runtime")
		}
		if c.ShutdownTimeoutSec != a.conf.ShutdownTimeoutSec {
			a.log.Debug(ctx, "Shutdown timeout updated",
				logger.Int("old", a.conf.ShutdownTimeoutSec),
				logger.Int("new", c.ShutdownTimeoutSec),
			)
			a.conf.ShutdownTimeoutSec = c.ShutdownTimeoutSec
		}
	})
}

func (a *application) run(ctx context.Context) error {
	var (
		cpu *os.File
		err error
	)

	if a.profile {
		cpu, err = os.Create("cpu.prof")
		if err != nil {
			return fmt.Errorf("failed to create CPU profile: %w", err)
		}

		if err = pprof.StartCPUProfile(cpu); err != nil {
			return fmt.Errorf("failed to start CPU profile: %w", err)
		}
	}
	defer func() {
		if a.profile {
			var heap *os.File
			a.log.Debug(ctx, "Saving CPU and memory profiles")

			heap, err = os.Create("heap.prof")
			if err != nil {
				a.log.Error(ctx, "Failed to create heap profile file", logger.Err(err))
				return
			}

			runtime.GC()
			if err = pprof.WriteHeapProfile(heap); err != nil {
				a.log.Error(ctx, "Failed to save heap profile", logger.Err(err))
			}

			pprof.StopCPUProfile()
			_ = heap.Close()
			_ = cpu.Close()
		}
	}()

	a.mu.RLock()
	metricsEnabled := a.conf.MetricsEnabled
	a.mu.RUnlock()

	if metricsEnabled {
		a.startMetricsServer(ctx)
	}

	a.startMQTTServer(ctx)

	waitSignal()
	a.log.Debug(ctx, "Stopping server")

	a.mu.RLock()
	shutdownTimeout := time.Duration(a.conf.ShutdownTimeoutSec) * time.Second
	a.mu.RUnlock()

	sdCtx, cancelSdCtx := context.WithTimeout(ctx, shutdownTimeout)
	defer cancelSdCtx()

	a.stopMQTTServer(sdCtx)

	if metricsEnabled {
		a.stopMetricsServer(sdCtx)
	}

	if sdCtx.Err() == nil {
		a.wg.Wait()
	}

	return nil
}

func (a *application) startMetricsServer(ctx context.Context) {
	a.metrics = metric.NewServer(ctx, a.log,
		metric.WithAddress(fmt.Sprintf("%s:%v", a.conf.MetricsHost, a.conf.MetricsPort)),
		metric.WithPath(a.conf.MetricsPath),
		metric.WithProfile(a.conf.MetricsProfiling),
	)

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		err := a.metrics.Serve()
		if err != nil {
			a.log.Error(ctx, "Failed to run metrics server", logger.Err(err))
		}
	}()
}

func (a *application) stopMetricsServer(ctx context.Context) {
	a.log.Debug(ctx, "Shutting down metrics server")

	if err := a.metrics.Shutdown(ctx); err != nil {
		a.log.Error(ctx, "Failed to shutdown metrics server", logger.Err(err))
		return
	}
}

func (a *application) startMQTTServer(ctx context.Context) {
	a.mqtt = mqtt.NewServer(ctx, a.log)
	_ = a.mqtt.AddListener(listener.NewTCP("tcp", ":1883", nil))

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		err := a.mqtt.Serve()
		if err != nil {
			a.log.Error(ctx, "Failed to run MQTT server", logger.Err(err))
		}
	}()
}

func (a *application) stopMQTTServer(ctx context.Context) {
	a.log.Debug(ctx, "Shutting down MQTT server")

	if err := a.mqtt.Shutdown(ctx); err != nil {
		a.log.Error(ctx, "Failed to shutdown MQTT server", logger.Err(err))
		return
	}
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
