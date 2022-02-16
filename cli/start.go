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

package cli

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dimiro1/banner"
	"github.com/gsalomao/maxmq/broker"
	"github.com/gsalomao/maxmq/config"
	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var bannerTemplate = `{{ .Title "MaxMQ" "" 0 }}
{{ .AnsiColor.BrightCyan }}  A Cloud-Native Message Broker for IoT
{{ .AnsiColor.Default }}
`

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

			err := config.ReadConfigFile()
			if err == nil {
				log.Info().Msg("Loading configuration from file")
			} else {
				if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
					log.Warn().Msg(err.Error())
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

			mqttConf := mqtt.Configuration{
				TCPAddress:      conf.MQTTTCPAddress,
				ConnectTimeout:  conf.MQTTConnectTimeout,
				BufferSize:      conf.MQTTBufferSize,
				MaxPacketSize:   conf.MQTTMaxPacketSize,
				MaxKeepAlive:    uint16(conf.MQTTMaxKeepAlive),
				MaximumQoS:      conf.MQTTMaximumQoS,
				RetainAvailable: conf.MQTTRetainAvailable,
				UserProperties:  conf.MQTTUserProperties,
			}

			cm := mqtt.NewConnectionManager(mqttConf, &log)
			r, err := mqtt.NewRunner(
				mqtt.WithConfiguration(mqttConf),
				mqtt.WithConnectionHandler(&cm),
				mqtt.WithLogger(&log),
			)
			if err != nil {
				log.Fatal().Msg("Failed to create MQTT runner: " +
					err.Error())
			}

			startBroker(r, &log)
		},
	}

	return cmd
}

func startBroker(r broker.Runner, log *logger.Logger) {
	brk := broker.New(log)
	brk.AddRunner(r)

	err := brk.Start()
	if err != nil {
		log.Error().Msg("Failed to start broker: " + err.Error())
	}

	go waitOSSignals(&brk)
	err = brk.Wait()
	if err != nil {
		log.Error().Msg("Broker stopped with error: " + err.Error())
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
