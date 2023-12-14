//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type Times struct {
	Setup    int
	Insert   int
	Copy     int
	Flatten  int
	Merge    int
	Shutdown int
}

type Request struct {
	key        []byte
	value      uint64
	operation  string
	ResponseCh chan Response
}

type Response struct {
	Error error
}

func TestMemtableConcurrentInsert(t *testing.T) {
	const numKeys = 1000
	const operationsPerClient = 10
	const numClients = 100
	numWorkers := runtime.NumCPU()

	operations := generateOperations(numKeys, operationsPerClient, numClients)

	t.Run(MEMTABLE_THREADED_BASELINE, func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_BASELINE, operations)
	})
	t.Run(MEMTABLE_THREADED_SINGLE_CHANNEL, func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_SINGLE_CHANNEL, operations)
	})

	t.Run(MEMTABLE_THREADED_RANDOM, func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_RANDOM, operations)
	})

	t.Run(MEMTABLE_THREADED_HASH, func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_HASH, operations)
	})
}

func RunExperiment(t *testing.T, numClients int, numWorkers int, workerAssignment string, operations [][]*Request) ([]*roaringset.BinarySearchNode, Times) {
	numChannels := numWorkers

	useThreadedFor := map[string]bool{
		StrategyRoaringSet: true,
	}

	if workerAssignment == MEMTABLE_THREADED_SINGLE_CHANNEL {
		numChannels = 1
	}

	if workerAssignment == MEMTABLE_THREADED_BASELINE {
		numChannels = 1
		numWorkers = 1
		useThreadedFor = map[string]bool{}
	}

	path := t.TempDir()
	strategy := StrategyRoaringSet

	m, err := newMemtableThreadedDebug(path, strategy, 0, nil, workerAssignment, useThreadedFor, numWorkers, 10)
	if err != nil {
		t.Fatal(err)
	}

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup

	requestsChannels := make([]chan Request, numChannels)

	for i := 0; i < numChannels; i++ {
		requestsChannels[i] = make(chan Request)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go client(i, numWorkers, operations[i], m, &wgClients, workerAssignment)
	}

	wgClients.Wait() // Wait for all clients to finish
	for _, ch := range requestsChannels {
		close(ch)
	}

	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	buckets := m.flattenNodesRoaringSet()

	times.Copy = int(time.Since(startTime).Milliseconds())

	fmt.Println("Results:", times.Setup)
	fmt.Println("\tSetup:", times.Setup)
	fmt.Println("\tInsert:", times.Insert)

	return buckets, times
}

func generateOperations(numKeys int, operationsPerClient int, numClients int) [][]*Request {
	keys := make([][]byte, numKeys)
	operations := make([][]*Request, numClients)
	operationsPerKey := make(map[string]int)

	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	for i := 0; i < numClients; i++ {
		operations[i] = make([]*Request, operationsPerClient)
		keyIndex := rand.Intn(len(keys))
		for j := 0; j < operationsPerClient; j++ {
			var funct string

			if rand.Intn(2) == 0 {
				funct = "RoaringSetAddOne"
			} else {
				funct = "RoaringSetRemoveOne"
			}
			operations[i][j] = &Request{key: keys[keyIndex], value: uint64(i*numClients + j), operation: funct}
			operationsPerKey[string(keys[keyIndex])]++
			keyIndex = (keyIndex + 1) % len(keys)
		}
	}
	fmt.Println("Operations:", numClients*operationsPerClient, "Unique key count:", len(operationsPerKey))
	return operations
}

func client(i int, numWorkers int, threadOperations []*Request, m *MemtableThreaded, wg *sync.WaitGroup, workerAssignment string) {
	defer wg.Done()

	for j := 0; j < len(threadOperations); j++ {
		if threadOperations[j].operation == "RoaringSetAddOne" {
			err := m.roaringSetAddOne(threadOperations[j].key, threadOperations[j].value)
			if err != nil {
				panic(err)
			}
		} else if threadOperations[j].operation == "RoaringSetRemoveOne" {
			err := m.roaringSetRemoveOne(threadOperations[j].key, threadOperations[j].value)
			if err != nil {
				panic(err)
			}
		}
	}
}