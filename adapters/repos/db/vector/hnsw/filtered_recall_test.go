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

//go:build benchmarkRecall
// +build benchmarkRecall

package hnsw

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestFilteredRecall(t *testing.T) {
	efConstruction := 256
	ef := 256
	maxNeighbors := 64

	type LabeledVector struct {
		Label  int       `json:"label"`
		Vector []float32 `json:"vector"`
	}

	var vectors []LabeledVector
	var queries []LabeledVector
	var truths [][]uint64
	var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		vectorsJSON, err := ioutil.ReadFile("filtered_recall_vectors.json")
		require.Nil(t, err)
		err = json.Unmarshal(vectorsJSON, &vectors)
		require.Nil(t, err)

		queriesJSON, err := ioutil.ReadFile("filtered_recall_queries.json")
		require.Nil(t, err)
		err = json.Unmarshal(queriesJSON, &queries)
		require.Nil(t, err)

		truthsJSON, err := ioutil.ReadFile("filtered_recall_truths.json")
		require.Nil(t, err)
		err = json.Unmarshal(truthsJSON, &truths)
		require.Nil(t, err)
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")

		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)].Vector, nil
			},
		}, ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
		}, cyclemanager.NewNoop())
		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][]LabeledVector, workerCount)

		before := time.Now()
		for i, vec := range vectors {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
		}

		wg := &sync.WaitGroup{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs []LabeledVector) {
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					err := vectorIndex.Add(uint64(originalIndex), vec.Vector) // change signature to add vec.Label
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("importing took %s\n", time.Since(before))
	})

	t.Run("inspect a query", func(t *testing.T) {
		k := 20

		hasDuplicates := 0

		for _, vec := range queries {
			results, _, err := vectorIndex.SearchByVector(vec.Vector, k, nil)
			require.Nil(t, err)
			if containsDuplicates(results) {
				hasDuplicates++
				panic("stop")
			}
		}

		fmt.Printf("%d out of %d searches contained duplicates\n", hasDuplicates, len(queries))
	})

	t.Run("with k=10", func(t *testing.T) {
		k := 10

		var relevant int
		var retrieved int

		for i := 0; i < len(queries); i++ {
			results, _, err := vectorIndex.SearchByVector(queries[i].Vector, k, nil)
			require.Nil(t, err)

			retrieved += k
			relevant += matchesInLists(truths[i], results)
		}

		recall := float32(relevant) / float32(retrieved)
		fmt.Printf("recall is %f\n", recall)
		assert.True(t, recall >= 0.09)
	})
}
