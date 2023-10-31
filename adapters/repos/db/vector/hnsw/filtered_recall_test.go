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

	"net/http"
	_ "net/http/pprof"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func init() {
	go func() {
		runtime.SetBlockProfileRate(1)
		http.ListenAndServe("localhost:6060", nil)
	}()
}

type Vector struct {
	ID     int       `json:"id"`
	Vector []float32 `json:"vector"`
}

type Filters struct {
	ID        int         `json:"id"`
	FilterMap map[int]int `json:"filterMap"`
}

type vecWithFilters struct {
	ID        int         `json:"id"`
	Vector    []float32   `json:"vector"`
	FilterMap map[int]int `json:"filterMap"`
}

type GroundTruths struct {
	QueryID int      `json:"queryID"`
	Truths  []uint64 `json:"truths"`
}

func TestFilteredRecall(t *testing.T) {
	efConstruction := 256
	ef := 256
	maxNeighbors := 64

	var indexVectors []Vector
	var indexFilters []Filters
	var queryVectors []Vector
	var queryFilters []Filters
	var truths []GroundTruths
	var vectorIndex *hnsw

	t.Run("Loading vectors for testing...", func(t *testing.T) {
		// vectors.json
		/*
			[{"id": 0, "vector": [0,15,35,...]},{"id": 0, "vector": [119,15,4,...]},...]
		*/
		indexVectorsJSON, err := ioutil.ReadFile("indexVectors_100K.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexVectorsJSON, &indexVectors)
		require.Nil(t, err)
		// vectorFilters.json
		/*
			[{"id": 0, "filterMap": {0: 1, 1: 3, ...}}, {"id": 1, "filterMap": {0: 2, 1: 4}}, ...]

			For now, running one test at a time, future - loop through filter paths
		*/
		indexFiltersJSON, err := ioutil.ReadFile("indexFilters-100K-2-95_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexFiltersJSON, &indexFilters)
		require.Nil(t, err)

		indexVectorsWithFilters := mergeData(indexVectors, indexFilters) // returns []vecWithFilters

		// Shuffle Index Vectors
		/*
			rand.Seed(42) // 42 = meaning of life
			rand.Shuffle(len(indexVectorsWithFilters), func(i, j int) {
				indexVectorsWithFilters[i], indexVectorsWithFilters[j] = indexVectorsWithFilters[j], indexVectorsWithFilters[i]
			})
		*/

		/* =================================================
			SAME JOINING OF VECTORS AND FILTERS FOR QUERIES
		   =================================================
		*/

		queryVectorsJSON, err := ioutil.ReadFile("queryVectors_100K.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryVectorsJSON, &queryVectors)
		require.Nil(t, err)

		queryFiltersJSON, err := ioutil.ReadFile("queryFilters-100K-2-95_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryFiltersJSON, &queryFilters)

		queryVectorsWithFilters := mergeData(queryVectors, queryFilters)

		truthsJSON, err := ioutil.ReadFile("filtered_recall_truths-100K-2-95_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(truthsJSON, &truths)
		require.Nil(t, err)

		fmt.Printf("importing into hnsw\n")

		//var cycleManagerNoop cyclemanager.CycleManager
		//cycleManagerNoop = cyclemanager.NewManagerNoop()
		var logger = logrus.New()
		tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstoneCallback", logger, 0)
		shardCompactionCallbacks := cyclemanager.NewCallbackGroup("shardCompactionCallback", logger, 0)
		shardFlushCallbacks := cyclemanager.NewCallbackGroup("shardFlushCallbacks", logger, 0)
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return indexVectorsWithFilters[int(id)].Vector, nil
			},
		}, ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
		}, tombstoneCallbacks, shardCompactionCallbacks, shardFlushCallbacks)

		filterToIDs := make(map[int]map[int][]uint64)

		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][]vecWithFilters, workerCount)

		before := time.Now()
		for i, vecWithFilters := range indexVectorsWithFilters {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vecWithFilters)
		}

		wg := &sync.WaitGroup{}
		mutex := &sync.Mutex{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs []vecWithFilters) {
				defer wg.Done() // shouldn't it just decrement one if this fails?
				for i, vec := range myJobs {
					/*
						if i%1_000 == 999 {
							fmt.Printf("\n Imported %d in %v \n", i, time.Since(before))
						}
					*/
					originalIndex := (i * workerCount) + workerID
					nodeId := uint64(originalIndex)

					/* TEST HNSW */
					err := vectorIndex.Add(nodeId, vec.Vector)
					require.Nil(t, err)

					// Local allowList mapping for queries
					mutex.Lock()
					// filterToIDs is now a map[int]map[int][]uint64
					for filter, filterValue := range vec.FilterMap {
						if _, ok := filterToIDs[filter]; !ok {
							filterToIDs[filter] = make(map[int][]uint64)
							filterToIDs[filter][filterValue] = []uint64{nodeId}
						} else {
							if _, ok := filterToIDs[filter][filterValue]; !ok {
								filterToIDs[filter][filterValue] = []uint64{nodeId}
							} else {
								filterToIDs[filter][filterValue] = append(filterToIDs[filter][filterValue], nodeId)
							}
						}
					}
					mutex.Unlock()

					/* TEST FILTERED HNSW */
					/*
						filterIdx := 0 // need to change the loop through the filters in the future, POC testing now
						mutex.Lock()
						insertAllowList := helpers.NewAllowList(filterToIDs[filterIdx][vec.FilterMap[filterIdx]]...)
						err := vectorIndex.HybridAdd(nodeId, vec.Vector, vec.FilterMap, 0.5, insertAllowList) // change signature to add vec.Label
						mutex.Unlock()
						fmt.Printf("\n Imported %d in %v \n", nodeId, time.Since(before))
						require.Nil(t, err)
					*/
				}
			}(workerID, jobs)
		}
		wg.Wait()

		fmt.Print("\n HERE! \n")

		//fmt.Printf("\n Average Matching Filters Per Node %v \n \n", AverageMatchingFiltersPerNode(vectorIndex, 0))

		fmt.Printf("importing took %s\n", time.Since(before))

		fmt.Printf("Adding edges post Index \n")

		indexAllowList := []uint64{}
		minorityFilterKey := 0
		minorityFilterValue := 1
		indexAllowIDs = append(indexAllowIDs, filterToIDs[minorityFilterKey][minorityFilterValue]...)
		indexAllowList := helpers.NewAllowList(indexAllowIDs...)

		/* Should also do this more concurrently than this */
		for idx, indexVecWithFilters := range indexVectorsWithFilters {
			if idx%1000 == 999 {
				fmt.Print(idx)
			}
			// Only add extra edges to the minority filter
				vectorIndex.AddFilteredEdges(uint64(indexVecWithFilters.ID), indexAllowList)
			}
		}

		fmt.Printf("Testing search With k=100 \n")

		k := 100

		var relevant_retrieved int
		var total_recall float32
		var total_latency float32

		/* Will need to think of how to generalize this logging to multiple filters */
		totalRecallPerFilter := make(map[int]float32)
		totalLatencyPerFilter := make(map[int]float32)
		totalCountPerFilter := make(map[int]float32)

		for i := 0; i < len(queryVectorsWithFilters); i++ {
			// change to queryFilters
			queryFilters := queryVectorsWithFilters[i].FilterMap

			allowListIDs := []uint64{}
			for filterKey, filterValue := range queryFilters {
				allowListIDs = append(allowListIDs, filterToIDs[filterKey][filterValue]...)
			}
			//construct an allowList from the []uint64 of ids that match the filter
			queryAllowList := helpers.NewAllowList(allowListIDs...)
			/* TEST FILTERED HNSW */

			// select a single filter for the entrypoint
			/*
				queryFilterKey := 0
				queryStart := time.Now()
				results, _, err := vectorIndex.SearchByVectorWithEPFilter(queryVectorsWithFilters[i].Vector, queryFilterKey, queryFilters[queryFilterKey], k, queryAllowList)
				//results, _, err := vectorIndex.SearchByVector(queryVectorsWithFilters[i].Vector, k, queryAllowList)
				local_latency := float32(time.Now().Sub(queryStart).Seconds())
			*/

			/* TEST HNSW */

			queryStart := time.Now()
			results, _, err := vectorIndex.SearchByVector(queryVectorsWithFilters[i].Vector, k, queryAllowList)
			local_latency := float32(time.Now().Sub(queryStart).Seconds())

			require.Nil(t, err)

			relevant_retrieved = matchesInLists(truths[i].Truths, results)
			local_recall := float32(relevant_retrieved) / 100 // might want to modify to len(Truths) for extreme filter cases
			total_recall += local_recall
			total_latency += local_latency
			for _, filter := range queryFilters {
				if _, ok := totalRecallPerFilter[filter]; !ok {
					totalRecallPerFilter[filter] = local_recall
					totalLatencyPerFilter[filter] = local_latency
					totalCountPerFilter[filter] = 1.0
				} else {
					totalRecallPerFilter[filter] += local_recall
					totalLatencyPerFilter[filter] += local_latency
					totalCountPerFilter[filter] += 1.0
				}
			}
		}

		average_recall := float32(total_recall) / float32(len(queryVectorsWithFilters))
		fmt.Printf("Average Recall for all filters = %f\n", average_recall)
		average_latency := float32(total_latency) / float32(len(queryVectorsWithFilters))
		fmt.Printf("Average Latency for all filters = %f\n", average_latency)

		/* Loop through query filters, adding new recall score */
		for filterKey, totalRecallValue := range totalRecallPerFilter {
			RecallPerFilter := totalRecallValue / totalCountPerFilter[filterKey]
			LatencyPerFilter := totalLatencyPerFilter[filterKey] / totalCountPerFilter[filterKey]
			fmt.Printf("Recall for filter %d = %f \n", filterKey, RecallPerFilter)
			fmt.Printf("Latency for filter %d = %f \n", filterKey, LatencyPerFilter)
		}

		// level = 0, future loop through levels maybe
		FilterMatchResults := AverageFilterMatchingEdgesPerLevel(vectorIndex, 0,
			filterToIDs, indexVectorsWithFilters)
		fmt.Print(FilterMatchResults)

		assert.True(t, average_recall >= 0.09)
	})
}

func mergeData(vectors []Vector, filters []Filters) []vecWithFilters {
	// Create a map for quick lookup of filters
	IDtoFilterMap := make(map[int]map[int]int)
	for _, filter := range filters {
		IDtoFilterMap[filter.ID] = filter.FilterMap
	}
	// Merge vectors and filters
	var results []vecWithFilters
	for _, vector := range vectors {
		result := vecWithFilters{
			ID:        vector.ID,
			Vector:    vector.Vector,
			FilterMap: IDtoFilterMap[vector.ID],
		}
		results = append(results, result)
	}

	return results
}

func matchesInLists(control []uint64, results []uint64) int {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches int
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}

func AverageFilterMatchingEdgesPerLevel(vectorIndex *hnsw, level int,
	filterToIDs map[int]map[int][]uint64, indexVecsWithFilters []vecWithFilters) map[int]map[int]float32 {
	filterFrequencyCounterMap := make(map[int]map[int]int)
	// hard-coded for speed
	filterFrequencyCounterMap[0] = make(map[int]int)
	filterFrequencyCounterMap[0][0] = 0
	filterFrequencyCounterMap[0][1] = 0
	globalMatchCounterMap := make(map[int]map[int]int)
	// again hard-coded
	globalMatchCounterMap[0] = make(map[int]int)
	globalMatchCounterMap[0][0] = 0
	globalMatchCounterMap[0][1] = 0
	for idx, indexVectorObj := range indexVecsWithFilters {
		if idx%10_000 == 9_999 {
			fmt.Print(idx)
			fmt.Print("\n")
		}
		// This gets more confusing with multiple filters
		// We are cheating right now because we are only measuring 2 filters
		allowListIDs := []uint64{}
		// there is only 1 filter in the FilterMap, so.. to be fixed later.
		for filterKey, filterValue := range indexVectorObj.FilterMap {
			allowListIDs = append(allowListIDs, filterToIDs[filterKey][filterValue]...)
			//construct an allowList from the []uint64 of ids that match the filter
			currFilterAllowList := helpers.NewAllowList(allowListIDs...)
			localMatchCounter := 0
			neighbors := vectorIndex.InspectNeighborsAtLevel(indexVectorObj.ID, level)
			for _, neighbor := range neighbors {
				if currFilterAllowList.Contains(neighbor) {
					localMatchCounter++
				}
			}
			globalMatchCounterMap[filterKey][filterValue] += localMatchCounter
			filterFrequencyCounterMap[filterKey][filterValue]++
		}
	}
	FilterMatchesMap := make(map[int]map[int]float32)
	FilterMatchesMap[0] = make(map[int]float32)
	FilterMatchesMap[0][0] = 0.0
	FilterMatchesMap[0][1] = 0.0
	for key, filterValueMap := range globalMatchCounterMap {
		for filterVal, count := range filterValueMap {
			averageMatches := float32(count) / float32(filterFrequencyCounterMap[key][filterVal])
			FilterMatchesMap[key][filterVal] = averageMatches
		}
	}
	return FilterMatchesMap
}
