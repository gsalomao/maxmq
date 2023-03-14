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
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/dimiro1/banner"
	"github.com/gsalomao/maxmq/internal/config"
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/metrics"
	"github.com/gsalomao/maxmq/internal/mqtt"
	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/server"
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
		Long:  "Start the execution of the MaxMQ server",
		Run: func(_ *cobra.Command, _ []string) {
			enableProfile := profile != ""
			runCommandStart(enableProfile)
		},
	}
}

func runCommandStart(enableProfile bool) {
	machineID := 0
	sf, err := snowflake.New(machineID)
	if err != nil {
		fmt.Println("failed to start server: " + err.Error())
		os.Exit(1)
	}

	var missingConfigFile bool
	err = config.ReadConfigFile()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			missingConfigFile = true
		} else {
			fmt.Println("failed to start server: " + err.Error())
			os.Exit(1)
		}
	}

	conf, err := config.LoadConfig()
	if err != nil {
		fmt.Println("failed to start server: " + err.Error())
		os.Exit(1)
	}

	baseLogger := logger.New(os.Stdout, sf, logger.LogFormat(conf.LogFormat))
	cmdLog := baseLogger.WithPrefix("cmd")

	err = logger.SetSeverityLevel(conf.LogLevel)
	if err != nil {
		cmdLog.Fatal().Msg("failed to set log severity: " + err.Error())
	}

	bannerWriter := colorable.NewColorableStdout()
	banner.InitString(bannerWriter, true, true, bannerTemplate)

	if !missingConfigFile {
		cmdLog.Info().Msg("config file loaded with success")
	} else {
		cmdLog.Info().Msg("no config file found")
	}

	var cf []byte
	cf, err = json.Marshal(conf)
	if err != nil {
		cmdLog.Fatal().Msg("failed to encode configuration: " + err.Error())
	}
	cmdLog.Debug().RawJSON("Configuration", cf).Msg("using configuration")

	s, err := newServer(conf, baseLogger, machineID)
	if err != nil {
		cmdLog.Fatal().Msg("failed to create server: " + err.Error())
	}

	startServer(s, cmdLog, enableProfile)
}

func newServer(c config.Config, l *logger.Logger, machineID int) (*server.Server, error) {
	mqttConf := handler.Configuration{
		TCPAddress:                    c.MQTTTCPAddress,
		ConnectTimeout:                c.MQTTConnectTimeout,
		BufferSize:                    c.MQTTBufferSize,
		DefaultVersion:                c.MQTTDefaultVersion,
		MaxPacketSize:                 c.MQTTMaxPacketSize,
		MaxKeepAlive:                  c.MQTTMaxKeepAlive,
		MaxSessionExpiryInterval:      c.MQTTSessionExpiration,
		MaxInflightMessages:           c.MQTTMaxInflightMessages,
		MaxInflightRetries:            c.MQTTMaxInflightRetries,
		MaximumQoS:                    c.MQTTMaximumQoS,
		MaxTopicAlias:                 c.MQTTMaxTopicAlias,
		RetainAvailable:               c.MQTTRetainAvailable,
		WildcardSubscriptionAvailable: c.MQTTWildcardSubscription,
		SubscriptionIDAvailable:       c.MQTTSubscriptionID,
		SharedSubscriptionAvailable:   c.MQTTSharedSubscription,
		MaxClientIDLen:                c.MQTTMaxClientIDLen,
		AllowEmptyClientID:            c.MQTTAllowEmptyClientID,
		ClientIDPrefix:                []byte(c.MQTTClientIDPrefix),
		UserProperties:                c.MQTTUserProperties,
		MetricsEnabled:                c.MetricsEnabled,
	}

	sf, err := snowflake.New(machineID)
	if err != nil {
		return nil, err
	}

	var lsn server.Listener
	lsn, err = mqtt.NewListener(
		mqtt.WithConfiguration(mqttConf),
		mqtt.WithLogger(l),
		mqtt.WithIDGenerator(sf),
	)
	if err != nil {
		return nil, err
	}

	s := server.New(l)
	s.AddListener(lsn)

	if c.MetricsEnabled {
		mtConf := metrics.Configuration{
			Address:   c.MetricsAddress,
			Path:      c.MetricsPath,
			Profiling: c.MetricsProfiling,
		}

		lsn, err = metrics.NewListener(mtConf, l)
		if err != nil {
			return nil, err
		}

		s.AddListener(lsn)
	}

	return &s, nil
}

func startServer(s *server.Server, l *logger.Logger, enableProfile bool) {
	if enableProfile {
		cpu, err := os.Create("cpu.prof")
		if err != nil {
			l.Fatal().Msg("failed to create CPU profile file: " + err.Error())
		}

		if err = pprof.StartCPUProfile(cpu); err != nil {
			l.Fatal().Msg("failed to start CPU profile: " + err.Error())
		}

		defer func() { _ = cpu.Close() }()
	}

	err := s.Start()
	if err != nil {
		l.Fatal().Msg("failed to start server: " + err.Error())
	}

	go waitOSSignals(s)

	err = s.Wait()
	if err != nil {
		l.Error().Msg("server stopped with error: " + err.Error())
	}

	if enableProfile {
		var heap *os.File

		heap, err = os.Create("heap.prof")
		if err != nil {
			l.Fatal().Msg("failed to create Heap profile file: " + err.Error())
		}
		defer func() { _ = heap.Close() }()

		runtime.GC()
		if err = pprof.WriteHeapProfile(heap); err != nil {
			l.Fatal().Msg("failed to save Heap profile: " + err.Error())
		}

		pprof.StopCPUProfile()
	}
}

func waitOSSignals(s *server.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	for {
		<-stop

		// Generates a new line to split the logs
		fmt.Println("")
		s.Stop()
	}
}
