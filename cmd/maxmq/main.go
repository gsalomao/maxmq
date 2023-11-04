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

package main

import (
	"context"
	"os"

	"github.com/gsalomao/maxmq/cmd/maxmq/command"
	"github.com/gsalomao/maxmq/internal/cli"
)

func main() {
	c := cli.New("maxmq",
		"MaxMQ is a Cloud-Native and High-Performance MQTT Broker for IoT.")

	c.AddCommand(command.NewStart())

	err := c.Run(context.Background(), os.Stdout, os.Args[1:])
	if err != nil {
		os.Exit(1)
	}
}
