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

	"github.com/gsalomao/maxmq/pkg/cli/command"
	"github.com/spf13/cobra"
)

// CLI represents the command line interface.
type CLI struct {
	rootCmd *cobra.Command
}

// version represents the application version and it's updated at build time.
var version = ""

// New creates an instance of the command line interface.
func New() CLI {
	cli := CLI{
		rootCmd: &cobra.Command{
			Use:     "maxmq",
			Version: version,
			Short:   "MaxMQ is a message broker for IoT",
			Long: `MaxMQ is a Cloud-Native Message Broker for IoT designed for High Performance
and High Availability.
`,
		},
	}

	cli.rootCmd.CompletionOptions.DisableDefaultCmd = true
	cli.rootCmd.SetVersionTemplate(fmt.Sprintf("MaxMQ version %v\n", version))
	cli.registerCommands()

	return cli
}

// Run executes the command line interface.
func (c CLI) Run() error {
	return c.rootCmd.Execute()
}

// registerCommands adds chield commands to the root command.
func (c *CLI) registerCommands() {
	c.rootCmd.AddCommand(command.NewCommandStart())
}
