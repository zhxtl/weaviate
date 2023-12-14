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
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func TestMemtableConcurrentMergeLoad(t *testing.T) {
	const numKeys = 1000
	const operationsPerClient = 10
	const numClients = 100
	numWorkers := runtime.NumCPU()

	operations := generateOperations(numKeys, operationsPerClient, numClients)
	correctOrder, err := createSimpleBucket(operations, t)
	require.Nil(t, err)

	t.Run(MEMTABLE_THREADED_BASELINE, func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_BASELINE, operations, correctOrder)
	})

	t.Run(MEMTABLE_THREADED_SINGLE_CHANNEL, func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_SINGLE_CHANNEL, operations, correctOrder)
	})

	t.Run(MEMTABLE_THREADED_RANDOM, func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_RANDOM, operations, correctOrder)
	})

	t.Run(MEMTABLE_THREADED_HASH, func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, MEMTABLE_THREADED_HASH, operations, correctOrder)
	})
}

func RunMergeExperiment(t *testing.T, numClients int, numWorkers int, workerAssignment string, operations [][]*Request, correctOrder []*roaringset.BinarySearchNode) []*roaringset.BinarySearchNode {
	nodes, times := RunExperiment(t, numClients, numWorkers, workerAssignment, operations)

	fmt.Println()
	fmt.Println("Concurrent buckets:")
	fmt.Println("\tSetup:", times.Setup)
	fmt.Println("\tInsert:", times.Insert)
	fmt.Println("\tCopy:", times.Copy)
	fmt.Println("\tMerge:", times.Merge)

	compareTime := time.Now()

	require.True(t, compareBuckets(correctOrder, nodes))

	fmt.Println("\tCompare buckets:", int(time.Since(compareTime).Milliseconds()))

	return nodes
}

func createSimpleBucket(operations [][]*Request, t *testing.T) ([]*roaringset.BinarySearchNode, error) {
	times := Times{}
	startTime := time.Now()

	dirName := t.TempDir()
	m, _ := newMemtable(dirName, StrategyRoaringSet, 0, nil)

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	maxOperations := 0
	for _, op := range operations {
		if len(op) > maxOperations {
			maxOperations = len(op)
		}
	}
	// transverse operations in column major order
	for i := 0; i < maxOperations; i++ {
		for j := 0; j < len(operations); j++ {
			if i >= len(operations[j]) {
				continue
			}
			if operations[j][i].operation == "RoaringSetAddOne" {
				err := m.roaringSetAddOne(operations[j][i].key, operations[j][i].value)
				require.Nil(t, err)
			} else if operations[j][i].operation == "RoaringSetRemoveOne" {
				err := m.roaringSetRemoveOne(operations[j][i].key, operations[j][i].value)
				require.Nil(t, err)
			}
		}
	}

	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	nodes := m.RoaringSet().FlattenInOrder()

	times.Flatten = int(time.Since(startTime).Milliseconds())

	fmt.Println("Single bucket with node count:", len(nodes))
	fmt.Println("\tSetup:", times.Setup)
	fmt.Println("\tInsert:", times.Insert)
	fmt.Println("\tFlatten:", times.Flatten)

	return nodes, nil
}

func compareBuckets(b1 []*roaringset.BinarySearchNode, b2 []*roaringset.BinarySearchNode) bool {
	if len(b1) != len(b2) {
		fmt.Println("Length not equal:", len(b1), len(b2))
		return false
	}
	for i := range b1 {
		if !bytes.Equal(b1[i].Key, b2[i].Key) {
			fmt.Println("Keys not equal:", string(b1[i].Key), string(b2[i].Key))
			return false
		}
		oldCardinality := b1[i].Value.Additions.GetCardinality()
		b1[i].Value.Additions.And(b2[i].Value.Additions)
		if b1[i].Value.Additions.GetCardinality() != oldCardinality {
			fmt.Println("Addition not equal:", string(b1[i].Key), oldCardinality, b2[i].Value.Additions.GetCardinality(), b1[i].Value.Additions.GetCardinality())
			return false
		}

		oldCardinality = b1[i].Value.Deletions.GetCardinality()
		b1[i].Value.Deletions.And(b2[i].Value.Deletions)
		if b1[i].Value.Deletions.GetCardinality() != oldCardinality {
			fmt.Println("Deletions not equal:", string(b1[i].Key), oldCardinality, b2[i].Value.Deletions.GetCardinality(), b1[i].Value.Deletions.GetCardinality())
			return false
		}

	}
	return true
}