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
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/dimiro1/banner"
	"github.com/gsalomao/maxmq/broker"
	"github.com/gsalomao/maxmq/config"
	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/metrics"
	"github.com/gsalomao/maxmq/mqtt"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var bannerTemplate = `{{ .Title "MaxMQ" "" 0 }}
{{ .AnsiColor.BrightCyan }}  A Cloud-Native Message Broker for IoT
{{ .AnsiColor.Default }}
`
var profile = ""

// NewCommandStart creates a command to start the message broker.
func newCommandStart() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start broker",
		Long:  "Start the execution of the MaxMQ broker",
		Run: func(_ *cobra.Command, _ []string) {
			banner.InitString(colorable.NewColorableStdout(), true, true,
				bannerTemplate)

			log := logger.New(os.Stdout)

			if profile != "" {
				cpu, err := startCPUProfile()
				if err != nil {
					log.Fatal().Msg("Failed to start CPU profile: " +
						err.Error())
				}
				defer func() { _ = cpu.Close() }()
			}

			conf, err := loadConfig(&log)
			if err != nil {
				log.Fatal().Msg("Failed to load configuration: " + err.Error())
			}

			err = logger.SetSeverityLevel(conf.LogLevel)
			if err != nil {
				log.Fatal().Msg("Failed to set log severity: " + err.Error())
			}

			brk, err := newBroker(conf, &log)
			if err != nil {
				log.Fatal().Msg("Failed to create broker: " + err.Error())
			}

			runBroker(brk, &log)

			if profile != "" {
				err := saveHeapProfile()
				if err != nil {
					log.Fatal().Msg("Failed to save memory profile: " +
						err.Error())
				}

				stopCPUProfile()
			}
		},
	}

	return cmd
}

func newBroker(conf config.Config, log *logger.Logger) (*broker.Broker, error) {
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

	cm := mqtt.NewConnectionManager(mqttConf, log)
	r, err := mqtt.NewRunner(
		mqtt.WithConfiguration(mqttConf),
		mqtt.WithConnectionHandler(&cm),
		mqtt.WithLogger(log),
	)
	if err != nil {
		return nil, err
	}

	brk := broker.New(log)
	brk.AddRunner(r)

	if conf.MetricsEnabled {
		log.Info().
			Str("Address", conf.MetricsAddress).
			Str("Path", conf.MetricsPath).
			Msg("Exporting metrics")

		c := metrics.Configuration{
			Address: conf.MetricsAddress,
			Path:    conf.MetricsPath,
		}

		prom, err := metrics.NewPrometheus(c, log)
		if err != nil {
			return nil, err
		}

		brk.AddRunner(prom)
	}

	return &brk, nil
}

func runBroker(brk *broker.Broker, log *logger.Logger) {
	err := brk.Start()
	if err != nil {
		log.Fatal().Msg("Failed to start broker: " + err.Error())
	}

	go waitOSSignals(brk)

	err = brk.Wait()
	if err != nil {
		log.Error().Msg("Broker stopped with error: " + err.Error())
	}
}

func loadConfig(log *logger.Logger) (config.Config, error) {
	err := config.ReadConfigFile()
	if err == nil {
		log.Info().Msg("Loading configuration")
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Warn().Msg(err.Error())
		}
	}

	return config.LoadConfig()
}

func startCPUProfile() (*os.File, error) {
	f, err := os.Create("cpu.prof")
	if err != nil {
		return nil, err
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		return nil, err
	}

	return f, nil
}

func stopCPUProfile() {
	pprof.StopCPUProfile()
}

func saveHeapProfile() error {
	f, err := os.Create("heap.prof")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return err
	}

	return nil
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
