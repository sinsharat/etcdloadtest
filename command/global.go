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

package command

import (
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/spf13/cobra"
)

var (
	rounds                 int           // total number of rounds the operation needs to be performed
	totalClientConnections int           // total number of client connections to be made with server
	noOfPrefixes           int           // total number of prefixes which will be watched upon
	watchPerPrefix         int           // number of watchers per prefix
	reqRate                int           // put request per second
	totalKeys              int           // total number of keys for operation
	totalConcurrentOp      int           // total no of concurrent operations to be performed for load
	runningTime            time.Duration // time for which operation should be performed
	keyLength              int           // Total length of key
	valueLength            int           // Total length of value
	consistencyType        string        // consistency for read operation
)

// GlobalFlags are flags that defined globally
// and are inherited to all sub-commands.
type GlobalFlags struct {
	Endpoints   []string
	DialTimeout time.Duration
}

func newClient(eps []string, timeout time.Duration) *clientv3.Client {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: time.Duration(timeout) * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func getClientConnections(cmd *cobra.Command, noOfConnections int) []*clientv3.Client {
	eps := endpointsFromFlag(cmd)
	dialTimeout := dialTimeoutFromCmd(cmd)
	clients := make([]*clientv3.Client, 0)
	totaleps := len(eps)
	for i := 0; i < noOfConnections; i++ {
		c := newClient([]string{eps[i%totaleps]}, dialTimeout)
		clients = append(clients, c)
	}
	return clients
}

func endpointsFromFlag(cmd *cobra.Command) []string {
	endpoints, err := cmd.Flags().GetStringSlice("endpoints")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return endpoints
}

func dialTimeoutFromCmd(cmd *cobra.Command) time.Duration {
	dialTimeout, err := cmd.Flags().GetDuration("dial-timeout")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return dialTimeout
}
