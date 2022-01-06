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

package main

import (
	"github.com/gsalomao/maxmq/pkg/config"
	"github.com/gsalomao/maxmq/pkg/logger"
)

func main() {
	log := logger.New()

	if err := config.ReadConfigFile(); err != nil {
		log.Warn().Msg(err.Error())
	}

	conf, err := config.LoadConfig()

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load the configuration")
	}

	logger.SetSeverityLevel(conf.LogLevel)
	log.Info().Msg("Configuration loaded with success")

	log.Debug().
		Str("LogLevel", conf.LogLevel).
		Msg("Using configuration")
}
