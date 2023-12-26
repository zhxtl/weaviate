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
	/* HNSW PARAMETERS */
	efConstruction := 256
	ef := 256
	maxNeighbors := 64

	/* DATA STUCTURES FOR TESTING */
	var indexVectors []Vector
	var indexFilters []Filters
	var queryVectors []Vector
	var queryFilters []Filters
	var truths []GroundTruths
	var vectorIndex *hnsw

	/* RUN TEST */
	t.Run("Loading vectors for testing...", func(t *testing.T) {
		/* READ VECTORS, FILTERS, AND GROUND TRUTHS FROM JSONS */
		/* USING THE SAME INDEX VECTORS FOR ALL FILTER LEVELS */
		indexVectorsJSON, err := ioutil.ReadFile("./datasets/filtered/indexVectors_100K.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexVectorsJSON, &indexVectors)
		require.Nil(t, err)
		/* ADD THE FILTERS -- TODO: TEST MORE THAN 1 FILTER % PER RUN */
		indexFiltersJSON, err := ioutil.ReadFile("./datasets/filtered/indexFilters-100K-2-99_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexFiltersJSON, &indexFilters)
		require.Nil(t, err)
		/*  MERGE INDEX VECTORS WITH FILTERS */
		indexVectorsWithFilters := mergeData(indexVectors, indexFilters)
		/* IDEA -- SHUFFLE VECTORS TO AVOID CONFOUNDING WITH INSERT ORDER */
		/* USE THE SAME QUERY VECTORS FOR ALL FILTER LEVELS */
		queryVectorsJSON, err := ioutil.ReadFile("./datasets/filtered/queryVectors_100K.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryVectorsJSON, &queryVectors)
		require.Nil(t, err)
		/* ADD THE FILTERS -- TODO: TEST MORE THAN 1 FILTER % PER RUN */
		queryFiltersJSON, err := ioutil.ReadFile("./datasets/filtered/queryFilters-100K-2-99_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryFiltersJSON, &queryFilters)
		/* MERGE QUERY VECTORS WITH FILTERS */
		queryVectorsWithFilters := mergeData(queryVectors, queryFilters)
		/* LOAD GROUND TRUTHS */
		truthsJSON, err := ioutil.ReadFile("./datasets/filtered/filtered_recall_truths-100K-2-99_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(truthsJSON, &truths)
		require.Nil(t, err)
		/* FINISHED LOADING VECTORS, FILTERS, and GROUNDTRUTHS */
		fmt.Printf("Finished loading vectors, now importing into HNSW...\n")
		/* INITIALIZE INDEX */
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-commitlogger-is-mocked-out",
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
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), newDummyStore(t))
		vectorIndex = index
		/* KEEPING THE FILTER TO ID MAPPING IN-MEMORY TO CONTSRUCT THE ALLOW LIST */
		filterToIDs := make(map[int]map[int][]uint64)
		IDsToFilter := make(map[uint64]map[int]int)
		IDsToVector := make(map[uint64][]float32)
		require.Nil(t, err)
		/* INSERT DATA INTO HNSW */
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
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					nodeId := uint64(originalIndex)
					err := vectorIndex.Add(nodeId, vec.Vector)
					require.Nil(t, err)
					/* INSERT INTO FILTER TO ID MAPPING */
					mutex.Lock()
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
					IDsToFilter[nodeId] = vec.FilterMap
					IDsToVector[nodeId] = vec.Vector
					mutex.Unlock()
				}
			}(workerID, jobs)
		}
		wg.Wait()
		fmt.Printf("Importing took %s \n", time.Since(before))
		/* ADDING FILTER SHARING NEIGHBORS AFTER THE GRAPH HAS BEEN BUILT */
		//var nodeFilter map[int]int
		//var nodeVec []float32
		addEdgesTimer := time.Now()
		for idx, node := range vectorIndex.nodes {
			if idx%1000 == 999 {
				fmt.Printf("\nCheckpoint at idx %d. Adding filter sharing edges has run for %s seconds.", idx, time.Since(addEdgesTimer))
			}
			// Get the Filter
			// nodeFilter := IDToFilter[node.id]
			// Or... node.filters (workaround for the test)
			if node == nil {
				fmt.Print("Nil node!!! \n")
				fmt.Print(idx)
				fmt.Print("\n Nil node!!!")
				break
			}
			nodeFilter, ok := IDsToFilter[node.id]
			if !ok {
				fmt.Print(node.id)
			}
			nodeAllowListIDs := []uint64{}
			for filterKey, filterValue := range nodeFilter {
				if ids, ok := filterToIDs[filterKey][filterValue]; ok {
					nodeAllowListIDs = append(nodeAllowListIDs, ids...)
				}
			}
			nodeAllowList := helpers.NewAllowList(nodeAllowListIDs...)
			// COUNT FILTER SHARING NEIGHBORS
			// IF LESS THAN K, RUN `addFilterSharingEdges`
			matchCount := countFilterSharingEdges(node, IDsToFilter, filterToIDs)
			if matchCount < 2 {
				fmt.Printf("\nNodeId: %d, has %f filter sharing neighbors.", node.id, matchCount)
				nodeVec := IDsToVector[node.id]
				vectorIndex.addFilterSharingEdges(nodeVec, node, nodeAllowList, 128)
				matchCount := countFilterSharingEdges(node, IDsToFilter, filterToIDs)
				fmt.Printf("\nAfter adding edges, NodeId: %d, now has %f filter sharing neighbors.", node.id, matchCount)
			}
			// ADD FILTERS FROM MAJORITY TO MINORITY

		}
		// TODO

		/* TEST RECALL AND LATENCY */
		/* SET K */
		k := 100
		/* SET DATA STRUCTURES */
		var relevant_retrieved int
		totalRecallPerFilter := make(map[int]float32)
		totalLatencyPerFilter := make(map[int]float32)
		totalCountPerFilter := make(map[int]float32)
		/* CALCULATE RECALL AND LATENCY */
		for i := 0; i < len(queryVectorsWithFilters); i++ {
			queryFilters := queryVectorsWithFilters[i].FilterMap
			allowListIDs := []uint64{}
			for filterKey, filterValue := range queryFilters {
				allowListIDs = append(allowListIDs, filterToIDs[filterKey][filterValue]...)
			}
			queryAllowList := helpers.NewAllowList(allowListIDs...)
			queryStart := time.Now()
			results, _, err := vectorIndex.SearchByVector(queryVectorsWithFilters[i].Vector, k, queryAllowList)
			require.Nil(t, err)
			local_latency := float32(time.Now().Sub(queryStart).Seconds())
			relevant_retrieved = matchesInLists(truths[i].Truths, results)
			/* FOR EXTERME FILTER CASES THERE MAY BE LESS THAN 100 -- REPLACE WITH `len(truths[i].Truths)` */
			local_recall := float32(relevant_retrieved) / 100
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
		for filterKey, totalRecallValue := range totalRecallPerFilter {
			RecallPerFilter := totalRecallValue / totalCountPerFilter[filterKey]
			LatencyPerFilter := totalLatencyPerFilter[filterKey] / totalCountPerFilter[filterKey]
			fmt.Printf("Recall for filter %d = %f \n", filterKey, RecallPerFilter)
			fmt.Printf("Latency for filter %d = %f \n", filterKey, LatencyPerFilter)
		}
		/* COUNT MATCHING NEIGHBORS PER FILTER */
		var matchLog map[int]map[int][]float32 = make(map[int]map[int][]float32)
		for idx, node := range vectorIndex.nodes {
			// LOGGER, THIS IS SLOW
			if idx%1000 == 999 {
				// TODO - MAKE THIS PRETTIER
				fmt.Printf("idx: %d \n", idx)
			}
			// CATCH NIL NODES IN `vectorIndex.nodes`
			if node == nil {
				// TODO - CLEAN THIS UP
				fmt.Print("Nil node!! \n")
				fmt.Print(idx)
				fmt.Print("Nil node!! \n")
				break
			}
			nodeFilter, ok := IDsToFilter[node.id]
			if !ok {
				fmt.Print(node.id)
				// can;t remember command for break put keep looping
			}
			count := countFilterSharingEdges(node, IDsToFilter, filterToIDs)
			for filterKey, filterValue := range nodeFilter {
				// INITIALIZE MAP ENTRY IF NIL
				if _, exists := matchLog[filterKey]; !exists {
					matchLog[filterKey] = make(map[int][]float32)
				}
				matchLog[filterKey][filterValue] = append(matchLog[filterKey][filterValue], count)
			}
		}
		// PRINT AVERAGE FILTER SHARING NEIGHBORS
		// `matchLog` to `matchAverage`
		var matchAverage map[int]map[int]float32 = make(map[int]map[int]float32)
		for filterKey, filterValueMap := range matchLog {
			if _, exists := matchAverage[filterKey]; !exists {
				matchAverage[filterKey] = make(map[int]float32)
			}
			for filterValue, counts := range filterValueMap {
				var total float32 = 0.0
				for _, count := range counts {
					total += count
				}
				matchAverage[filterKey][filterValue] = total / float32(len(counts))
			}
		}
		// DISPLAY COUNT AVERAGES
		for filterKey, filterValueMap := range matchAverage {
			for filterValue, avgLatency := range filterValueMap {
				fmt.Printf("\nFilter [%d:%d] Average Matching Neighbors: %f\n", filterKey, filterValue, avgLatency)
			}
		}
	})
}

// RUN FOR ONE NODE
func countFilterSharingEdges(node *vertex, IDstoFilter map[uint64]map[int]int, filterToIDs map[int]map[int][]uint64) float32 {
	// GET FILTER FOR NODE
	nodeFilter, ok := IDstoFilter[node.id]
	if !ok {
		fmt.Print(node.id)
	}
	// CONSTRUCT ALLOWLIST FOR NODE
	nodeAllowListIDs := []uint64{}
	for filterKey, filterValue := range nodeFilter {
		nodeAllowListIDs = append(nodeAllowListIDs, filterToIDs[filterKey][filterValue]...)
	}
	nodeAllowList := helpers.NewAllowList(nodeAllowListIDs...)
	var count float32 = 0.0
	for _, connection := range node.connections[0] {
		if nodeAllowList.Contains(connection) {
			count += 1.0
		}
	}
	return count
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
