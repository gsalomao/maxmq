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
	"io"

	"github.com/gsalomao/maxmq/internal/build"
	"github.com/spf13/cobra"
)

// CLI represents the command line interface.
type CLI struct {
	rootCmd *cobra.Command
}

// New creates an instance of the command line interface.
func New(out io.Writer, args []string) CLI {
	description := "MaxMQ is a Cloud-Native and High-Performance MQTT Broker for IoT."
	cli := CLI{
		rootCmd: &cobra.Command{
			Use:     "maxmq",
			Version: build.GetInfo().ShortVersion(),
			Short:   "MaxMQ is a message broker for IoT",
			Long:    description,
		},
	}

	cli.rootCmd.CompletionOptions.DisableDefaultCmd = true
	cli.rootCmd.SetVersionTemplate("{{printf .Version}}")
	cli.rootCmd.SetArgs(args)
	cli.rootCmd.SetOut(out)
	cli.registerSubCommands()

	return cli
}

// Run executes the command line interface.
func (c *CLI) Run() error {
	return c.rootCmd.Execute()
}

func (c *CLI) registerSubCommands() {
	c.rootCmd.AddCommand(newCommandStart())
	c.rootCmd.AddCommand(newCommandVersion())
}
