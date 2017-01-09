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
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
)

var (
	mode string
)

func NewLoadPURCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "TestPUR",
		Short: "Performs load test for put, update and read(get) operations",
		Run:   performLoadPURFunc,
	}

	cmd.Flags().IntVar(&rounds, "rounds", 1, "No of cycle for which the operation is to be performed.")
	cmd.Flags().IntVar(&totalConcurrentOp, "total-concurrent-ops", 10, "total no of concuurent operations to be performed.")
	cmd.Flags().IntVar(&totalClientConnections, "total-client", 10, "total no of client connections to use")
	cmd.Flags().StringVar(&mode, "mode", "all", "all, put, update, get")
	cmd.Flags().IntVar(&noOfPrefixes, "total-prefixes", 10, "total no of unique prefixes to use")
	cmd.Flags().IntVar(&totalKeys, "total-keys", 1000, "total number of keys to watch")
	cmd.Flags().IntVar(&keyLength, "key-length", 64, "length of key for the operation")
	cmd.Flags().IntVar(&valueLength, "value-length", 64, "length of value for the operation")
	cmd.Flags().StringVar(&consistencyType, "consistency", "l", "Linearizable(l) or Serializable(s)")
	return cmd
}

func performLoadPURFunc(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		ExitWithError(ExitBadFeature, errors.New("loadPUT command does not support any arguments."))
	}

	if totalConcurrentOp > totalKeys {
		ExitWithError(ExitBadFeature, errors.New("total-concurrent-ops should be less than or equal to total-keys."))
	}

	if keyLength < 1 || valueLength < 1 {
		ExitWithError(ExitBadFeature, errors.New("key-length and value-length should greater than 0."))
	}

	if totalKeys%noOfPrefixes != 0 {
		ExitWithError(ExitBadFeature, errors.New("total-keys should be a multiple of total-prefixes."))
	}

	var cycles int
	switch mode {
	case "all":
		cycles = 2
	case "put":
		cycles = 1
	case "update":
		cycles = 2
	case "get":
		cycles = 1
	default:
		ExitWithError(ExitBadFeature, errors.New("Invalid mode."))
	}

	for round := 0; round < rounds; round++ {
		runPUR(cmd, cycles, round)
	}
}

func runPUR(cmd *cobra.Command, cycles, round int) {
	ctx := context.Background()
	keyPerPrefix := totalKeys / noOfPrefixes
	prefixLength := keyLength / 2
	keyPrefixes := UniqueStrings(uint(prefixLength), noOfPrefixes)
	keySuffixes := RandomStrings(uint(keyLength-prefixLength), keyPerPrefix)
	values := RandomStrings(uint(valueLength), totalKeys)
	keys := make([]string, 0)

	for _, keyPrefix := range keyPrefixes {
		for _, keySuffix := range keySuffixes {
			keys = append(keys, keyPrefix+keySuffix)
		}
	}

	cl := getClientConnections(cmd, 1)[0]
	defer cl.Close()

	var (
		wg     sync.WaitGroup
		err    error
		ctxt   context.Context
		cancel func()
	)

	rcs := getClientConnections(cmd, totalClientConnections)

	keysPerConcOp := int(totalKeys / totalConcurrentOp)
	if totalKeys%totalConcurrentOp != 0 {
		keysPerConcOp += 1
	}

	clientsPerConn := int(totalClientConnections / totalConcurrentOp)

	ctxt, cancel = context.WithCancel(ctx)
	defer cancel()

	var subKeys []string
	var subValues []string
	var subsclient []*clientv3.Client
	for limit := 0; limit < cycles; limit++ {
		stp := time.Now()
		wg.Add(totalConcurrentOp)
		for i := 0; i < totalConcurrentOp; i++ {
			start := i * keysPerConcOp
			end := (i * keysPerConcOp) + keysPerConcOp
			if len(keys) < end {
				subKeys = keys[start:]
				subValues = values[start:]
			} else {
				subKeys = keys[start:end]
				subValues = values[start:end]
			}

			startc := i * clientsPerConn
			endc := (i * clientsPerConn) + clientsPerConn
			if len(rcs) < endc {
				subsclient = rcs[startc:]
			} else {
				subsclient = rcs[startc:endc]
			}

			go func(subKeys []string, subValues []string, subsclient []*clientv3.Client) {
				defer wg.Done()
				clientIndex := 0
				for i, key := range subKeys {
					if clientIndex >= len(subsclient) {
						clientIndex = 0
					}
					client := subsclient[clientIndex]
					if _, err = client.Put(ctxt, key, subValues[i]); err != nil {
						log.Fatalf("failed to put key: %v, got err : ", key, err)
					}
					clientIndex++
				}
			}(subKeys, subValues, subsclient)
		}
		wg.Wait()
		if limit == 0 && (mode == "all" || mode == "put") {
			fmt.Printf("round %v: Time taken for put for keys: %v is : %v\n", round, totalKeys, time.Since(stp))
		} else if limit > 0 {
			fmt.Printf("round %v: Time taken for update for keys: %v is : %v\n", round, totalKeys, time.Since(stp))
		}

	}

	if mode == "get" || mode == "all" {
		opts := []clientv3.OpOption{}
		switch consistencyType {
		case "s":
			opts = append(opts, clientv3.WithSerializable())
		case "l":
		default:
			ExitWithError(ExitBadFeature, fmt.Errorf("unknown consistency flag %q", consistencyType))
		}

		str := time.Now()
		wg.Add(totalConcurrentOp)
		for i := 0; i < totalConcurrentOp; i++ {
			start := i * keysPerConcOp
			end := (i * keysPerConcOp) + keysPerConcOp
			if len(keys) < end {
				subKeys = keys[start:]
			} else {
				subKeys = keys[start:end]
			}

			startc := i * clientsPerConn
			endc := (i * clientsPerConn) + clientsPerConn
			if len(rcs) < endc {
				subsclient = rcs[startc:]
			} else {
				subsclient = rcs[startc:endc]
			}

			go func(subKeys []string, subsclient []*clientv3.Client) {
				defer wg.Done()
				clientIndex := 0
				for _, key := range subKeys {
					if clientIndex >= len(subsclient) {
						clientIndex = 0
					}
					client := subsclient[clientIndex]
					if _, err = client.Get(ctxt, key, opts...); err != nil {
						log.Fatalf("failed to get key: %v, got err : ", key, err)
					}
					clientIndex++
				}
			}(subKeys, subsclient)
		}
		wg.Wait()
		fmt.Printf("round %v: Time taken for get for keys : %v, is : %v\n", round, totalKeys, time.Since(str))
	}

	for _, rc := range rcs {
		rc.Close()
	}

	for _, keyPrefix := range keyPrefixes {
		if err = deletePrefix(ctx, cl, keyPrefix); err != nil {
			log.Fatalf("failed to clean up keys after test: %v", err)
		}
	}

}
