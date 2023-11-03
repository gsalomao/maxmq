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

package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/snowflake"
	"github.com/spf13/cobra"
)

// NewStart returns a command to start the server.
func NewStart() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start server",
		Long:  "Start the MaxMQ server.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return startServer(cmd.Context(), cmd.OutOrStdout(), 0)
		},
	}
}

func startServer(ctx context.Context, w io.Writer, machineID int) error {
	ctx = logger.Context(ctx, logger.Int("machine_id", machineID))

	sf := snowflake.New(machineID)
	log := logger.New(w, &logger.Options{
		Level:          logger.LevelDebug,
		Format:         logger.FormatPretty,
		LogIDGenerator: sf,
	})

	log.Debug(ctx, "Starting server")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	<-sigs
	fmt.Println()

	log.Debug(ctx, "Stopping server")
	return nil
}
