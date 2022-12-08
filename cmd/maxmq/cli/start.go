// Copyright 2022 The MaxMQ Authors
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
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/dimiro1/banner"
	"github.com/gsalomao/maxmq/internal/broker"
	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/metrics"
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
		Short: "Start broker",
		Long:  "Start the execution of the MaxMQ broker",
		Run: func(_ *cobra.Command, _ []string) {
			enableProfile := profile != ""
			runCommandStart(enableProfile)
		},
	}
}

func runCommandStart(enableProfile bool) {
	machineID := 0
	log, err := newLogger(os.Stdout, machineID)
	if err != nil {
		os.Exit(1)
	}

	var missingConfigFile bool
	err = config.ReadConfigFile()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			missingConfigFile = true
		} else {
			log.Fatal().Msg("Failed to read config file: " + err.Error())
		}
	}

	conf, err := config.LoadConfig()
	if err != nil {
		log.Fatal().Msg("Failed to load configuration: " + err.Error())
	}

	err = logger.SetSeverityLevel(conf.LogLevel)
	if err != nil {
		log.Fatal().Msg("Failed to set log severity: " + err.Error())
	}

	bannerWriter := colorable.NewColorableStdout()
	banner.InitString(bannerWriter, true, true, bannerTemplate)

	if !missingConfigFile {
		log.Info().Msg("Config file loaded with success")
	} else {
		log.Info().Msg("No config file found")
	}

	b, err := newBroker(conf, log, machineID)
	if err != nil {
		log.Fatal().Msg("Failed to create broker: " + err.Error())
	}

	runBroker(b, log, enableProfile)
}

func newLogger(out io.Writer, machineID int) (*logger.Logger, error) {
	sf, err := snowflake.New(machineID)
	if err != nil {
		return nil, err
	}

	lg := logger.New(out, sf)
	return &lg, nil
}

func newBroker(conf config.Config, log *logger.Logger,
	machineID int) (*broker.Broker, error) {

	mqttConf := mqtt.Configuration{
		TCPAddress:                    conf.MQTTTCPAddress,
		ConnectTimeout:                conf.MQTTConnectTimeout,
		BufferSize:                    conf.MQTTBufferSize,
		MaxPacketSize:                 conf.MQTTMaxPacketSize,
		MaxKeepAlive:                  conf.MQTTMaxKeepAlive,
		MaxSessionExpiryInterval:      conf.MQTTSessionExpiration,
		MaxInflightMessages:           conf.MQTTMaxInflightMessages,
		MaximumQoS:                    conf.MQTTMaximumQoS,
		MaxTopicAlias:                 conf.MQTTMaxTopicAlias,
		RetainAvailable:               conf.MQTTRetainAvailable,
		WildcardSubscriptionAvailable: conf.MQTTWildcardSubscription,
		SubscriptionIDAvailable:       conf.MQTTSubscriptionID,
		SharedSubscriptionAvailable:   conf.MQTTSharedSubscription,
		MaxClientIDLen:                conf.MQTTMaxClientIDLen,
		AllowEmptyClientID:            conf.MQTTAllowEmptyClientID,
		ClientIDPrefix:                []byte(conf.MQTTClientIDPrefix),
		UserProperties:                conf.MQTTUserProperties,
		MetricsEnabled:                conf.MetricsEnabled,
	}

	sf, err := snowflake.New(machineID)
	if err != nil {
		return nil, err
	}

	var lsn broker.Listener
	lsn, err = mqtt.NewListener(
		mqtt.WithConfiguration(mqttConf),
		mqtt.WithLogger(log),
		mqtt.WithIDGenerator(sf),
	)
	if err != nil {
		return nil, err
	}

	b := broker.New(log)
	b.AddListener(lsn)

	if conf.MetricsEnabled {
		log.Debug().
			Str("Address", conf.MetricsAddress).
			Str("Path", conf.MetricsPath).
			Msg("Exporting metrics")

		mtConf := metrics.Configuration{
			Address:   conf.MetricsAddress,
			Path:      conf.MetricsPath,
			Profiling: conf.MetricsProfiling,
		}

		lsn, err = metrics.NewListener(mtConf, log)
		if err != nil {
			return nil, err
		}

		b.AddListener(lsn)
	}

	return &b, nil
}

func runBroker(b *broker.Broker, log *logger.Logger, enableProfile bool) {
	if enableProfile {
		cpu, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal().Msg("Failed to create CPU profile file: " + err.Error())
		}

		if err = pprof.StartCPUProfile(cpu); err != nil {
			log.Fatal().Msg("Failed to start CPU profile: " + err.Error())
		}

		defer func() { _ = cpu.Close() }()
	}

	err := b.Start()
	if err != nil {
		log.Fatal().Msg("Failed to start broker: " + err.Error())
	}

	go waitOSSignals(b)

	err = b.Wait()
	if err != nil {
		log.Error().Msg("Broker stopped with error: " + err.Error())
	}

	if enableProfile {
		var heap *os.File

		heap, err = os.Create("heap.prof")
		if err != nil {
			log.Fatal().Msg("Failed to create Heap profile file: " +
				err.Error())
		}
		defer func() { _ = heap.Close() }()

		runtime.GC()
		if err = pprof.WriteHeapProfile(heap); err != nil {
			log.Fatal().Msg("Failed to save Heap profile: " + err.Error())
		}

		pprof.StopCPUProfile()
	}
}

func waitOSSignals(brk *broker.Broker) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	for {
		<-stop

		// Generates a new line to split the logs
		fmt.Println("")
		brk.Stop()
	}
}
