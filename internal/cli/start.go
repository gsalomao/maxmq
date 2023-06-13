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

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/dimiro1/banner"
	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt"
	"github.com/gsalomao/maxmq/internal/snowflake"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var bannerTemplate = `{{ .Title "MaxMQ" "" 0 }}
{{ .AnsiColor.BrightCyan }}  A Cloud-Native Message Broker for IoT
{{ .AnsiColor.Default }}
`
var profile = ""

func newCommandStart() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start server",
		Long:  "Start the MaxMQ server",
		Run: func(_ *cobra.Command, _ []string) {
			profileEnabled := profile != ""

			conf, confFileFound, err := loadConfig()
			if err != nil {
				fmt.Println("Failed to start server: " + err.Error())
				os.Exit(1)
			}

			var log *logger.Logger
			log, err = newLogger(logger.LogFormat(conf.LogFormat), conf.LogLevel, conf.MachineID)
			if err != nil {
				fmt.Println("Failed to start server: " + err.Error())
				os.Exit(1)
			}

			bannerWriter := colorable.NewColorableStdout()
			banner.InitString(bannerWriter, true, true, bannerTemplate)

			ctx, cancel := context.WithCancel(context.Background())

			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-sigs
				fmt.Println("")
				cancel()
			}()

			runServer(ctx, conf, confFileFound, profileEnabled, log)
		},
	}
}

func newLogger(format logger.LogFormat, level string, machineID int) (l *logger.Logger, err error) {
	err = logger.SetSeverityLevel(level)
	if err != nil {
		return nil, err
	}

	var gen logger.LogIDGenerator
	gen, err = snowflake.New(machineID)
	if err != nil {
		return nil, err
	}

	return logger.New(os.Stdout, gen, format), nil
}

func loadConfig() (c config.Config, found bool, err error) {
	err = config.ReadConfigFile()
	if err == nil {
		found = true
	} else if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
		return c, found, err
	}

	c, err = config.LoadConfig()
	return c, found, err
}

func runServer(ctx context.Context, c config.Config, confFileFound, profileEnabled bool, log *logger.Logger) {
	bsLog := log.WithPrefix("bootstrap")
	if confFileFound {
		bsLog.Info().Msg("Config file loaded with success")
	} else {
		bsLog.Info().Msg("No config file found")
	}

	if cf, err := json.Marshal(c); err == nil {
		bsLog.Debug().RawJSON("Configuration", cf).Msg("Using configuration")
	} else {
		bsLog.Fatal().Err(err).Msg("Failed to encode configuration")
	}

	var err error
	var cpu *os.File

	if profileEnabled {
		cpu, err = os.Create("cpu.prof")
		if err != nil {
			bsLog.Fatal().Err(err).Msg("Failed to create CPU profile file")
		}

		if err = pprof.StartCPUProfile(cpu); err != nil {
			bsLog.Fatal().Err(err).Msg("Failed to start CPU profile")
		}
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		err = runMQTTServer(ctx, c, log)
		if err != nil {
			bsLog.Fatal().Err(err).Msg("Failed to run MQTT server")
		}
	}()

	wg.Wait()

	if profileEnabled {
		var heap *os.File
		heap, err = os.Create("heap.prof")
		if err != nil {
			bsLog.Fatal().Err(err).Msg("Failed to create Heap profile file")
		}

		runtime.GC()
		if err = pprof.WriteHeapProfile(heap); err != nil {
			bsLog.Fatal().Err(err).Msg("Failed to save Heap profile")
		}

		pprof.StopCPUProfile()
		_ = heap.Close()
		_ = cpu.Close()
	}
}

func runMQTTServer(ctx context.Context, c config.Config, log *logger.Logger) error {
	mqttConf := &mqtt.Config{
		TCPAddress:                    c.MQTTTCPAddress,
		BufferSize:                    c.MQTTBufferSize,
		MaxPacketSize:                 c.MQTTMaxPacketSize,
		MaxSessionExpiryInterval:      c.MQTTMaxSessionExpiryInterval,
		MaxMessageExpiryInterval:      c.MQTTMaxMessageExpiryInterval,
		SysTopicUpdateInterval:        c.MQTTSysTopicUpdateInterval,
		MaxOutboundMessages:           c.MQTTMaxOutboundMessages,
		ReceiveMaximum:                c.MQTTReceiveMaximum,
		MaximumQoS:                    c.MQTTMaximumQoS,
		MaxTopicAlias:                 c.MQTTMaxTopicAlias,
		RetainAvailable:               c.MQTTRetainAvailable,
		WildcardSubscriptionAvailable: c.MQTTWildcardSubscriptionAvailable,
		SubscriptionIDAvailable:       c.MQTTSubscriptionIDAvailable,
		SharedSubscriptionAvailable:   c.MQTTSharedSubscriptionAvailable,
		MinProtocolVersion:            c.MQTTMinProtocolVersion,
		MetricsEnabled:                c.MetricsEnabled,
	}

	srv, _ := mqtt.NewServer(
		mqtt.WithConfig(mqttConf),
		mqtt.WithLogger(log.WithPrefix("mqtt")),
		mqtt.WithMachineID(c.MachineID),
	)

	err := srv.Start()
	if err != nil {
		return err
	}

	<-ctx.Done()

	srv.Stop()
	return nil
}
