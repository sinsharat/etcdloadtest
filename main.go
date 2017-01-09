// Copyright 2017 Huawei Technoligies
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

// etcdloadtest is a command line application that performs load tests on etcd.
package main

import (
	"log"
	"time"

	"github.com/sinsharat/etcdloadtest/command"
	"github.com/spf13/cobra"
)

const (
	cliName        = "etcdloadtest"
	cliDescription = "etcdloadtest performs load test using clientv3 functionality.."

	defaultDialTimeout = 2 * time.Second
)

var (
	globalFlags = command.GlobalFlags{}
)

var (
	rootCmd = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"etcdloadtest"},
	}
)

func init() {
	log.SetFlags(log.Lmicroseconds)
	rootCmd.PersistentFlags().StringSliceVar(&globalFlags.Endpoints, "endpoints", []string{"127.0.0.1:2379"}, "gRPC endpoints")
	rootCmd.PersistentFlags().DurationVar(&globalFlags.DialTimeout, "dial-timeout", defaultDialTimeout, "dial timeout for client connections")

	rootCmd.AddCommand(
		command.NewLoadPURCommand(),
		command.NewLoadWatchCommand(),
	)
}

func init() {
	cobra.EnablePrefixMatching = true
}

func Start() {
	rootCmd.SetUsageFunc(usageFunc)

	// Make help just show the usage
	rootCmd.SetHelpTemplate(`{{.UsageString}}`)

	if err := rootCmd.Execute(); err != nil {
		command.ExitWithError(command.ExitError, err)
	}
}

func main() {
	Start()
}
