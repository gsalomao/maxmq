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

package cli

import (
	"context"
	"io"

	"github.com/gsalomao/maxmq/internal/info"
	"github.com/spf13/cobra"
)

// CLI represents the command line interface.
type CLI struct {
	rootCmd *cobra.Command
}

// New creates an instance of the command line interface.
func New(name, description string) CLI {
	build := info.GetBuild()
	c := CLI{
		rootCmd: &cobra.Command{
			Use:     name,
			Version: build.ShortVersion(),
			Long:    description,
		},
	}

	c.rootCmd.CompletionOptions.DisableDefaultCmd = true
	c.rootCmd.SetVersionTemplate("{{printf .Use}} {{printf .Version}}")

	c.rootCmd.AddCommand(&cobra.Command{
		Use:                   "version",
		Short:                 "Show version and build summary",
		Long:                  "Show version and build summary.",
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := cmd.OutOrStdout().Write([]byte(build.LongVersion()))
			return err
		},
	})

	return c
}

// AddCommand adds a command to the CLI.
func (c CLI) AddCommand(cmd *cobra.Command) {
	c.rootCmd.AddCommand(cmd)
}

// Run executes the command line interface.
func (c CLI) Run(ctx context.Context, w io.Writer, args []string) error {
	c.rootCmd.SetOut(w)
	c.rootCmd.SetArgs(args)
	return c.rootCmd.ExecuteContext(ctx)
}
