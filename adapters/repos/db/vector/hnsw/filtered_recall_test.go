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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
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

var HNSW_EFG *bool

func init() {
	HNSW_EFG = flag.Bool("HNSW_EFG", false, "Enable HNSW-EFG")
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
	/* GET HNSW-EFG TOGGLE FROM CLI */
	flag.Parse()
	var hnsw_efg bool
	if *HNSW_EFG {
		fmt.Println("Running HNSW-EFG")
		hnsw_efg = true
	} else {
		fmt.Println("Running HNSW without EFG")
		hnsw_efg = false
	}
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
		indexVectorsJSON, err := ioutil.ReadFile("./datasets/filtered/indexVectors-1M.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexVectorsJSON, &indexVectors)
		require.Nil(t, err)
		/* ADD THE FILTERS -- TODO: TEST MORE THAN 1 FILTER % PER RUN */
		indexFiltersJSON, err := ioutil.ReadFile("./datasets/filtered/indexFilters-1M-2-95_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexFiltersJSON, &indexFilters)
		require.Nil(t, err)
		/*  MERGE INDEX VECTORS WITH FILTERS */
		indexVectorsWithFilters := mergeData(indexVectors, indexFilters)
		/* IDEA -- SHUFFLE VECTORS TO AVOID CONFOUNDING WITH INSERT ORDER */
		/* USE THE SAME QUERY VECTORS FOR ALL FILTER LEVELS */
		queryVectorsJSON, err := ioutil.ReadFile("./datasets/filtered/queryVectors-1M.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryVectorsJSON, &queryVectors)
		require.Nil(t, err)
		/* ADD THE FILTERS -- TODO: TEST MORE THAN 1 FILTER % PER RUN */
		queryFiltersJSON, err := ioutil.ReadFile("./datasets/filtered/queryFilters-1M-2-95_0.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryFiltersJSON, &queryFilters)
		/* MERGE QUERY VECTORS WITH FILTERS */
		queryVectorsWithFilters := mergeData(queryVectors, queryFilters)
		/* LOAD GROUND TRUTHS */
		truthsJSON, err := ioutil.ReadFile("./datasets/filtered/filtered-recall-truths-1M-2-95_0.json")
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
		/* KEEPING THE FILTER TO ID MAPPING IN-MEMORY TO CONSTRUCT THE ALLOW LIST */
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
		// Turn off Interventions here to record original Latency / Recall
		if hnsw_efg {
			addEdgesTimer := time.Now()
			// TODO, replace with deriving from data
			minorityFilter := map[int]int{0: 1}
			// ToDo Add Multi-Threaded Graph Repair
			workerCount = runtime.GOMAXPROCS(0)
			jobsForGraphRepairWorker := make([][]*vertex, workerCount)
			for idx, node := range vectorIndex.nodes {
				if node == nil {
					fmt.Printf("Nil node at idx %d! \n", idx)
				} else {
					workerID := idx % workerCount
					jobsForGraphRepairWorker[workerID] = append(jobsForGraphRepairWorker[workerID], node)
				}
			}
			wgForGraphRepair := &sync.WaitGroup{}
			mutexForGraphRepair := &sync.Mutex{}
			for workerID, jobs := range jobsForGraphRepairWorker {
				wgForGraphRepair.Add(1)
				go func(workerID int, myJobs []*vertex) {
					defer wgForGraphRepair.Done()
					for _, node := range myJobs {
						nodeFilter, ok := IDsToFilter[node.id]
						if !ok {
							fmt.Printf("Couldn't get filter for node %d", node.id)
						}
						nodeAllowList := buildAllowList(nodeFilter, filterToIDs)
						// COUNT FILTER SHARING NEIGHBORS
						// IF LESS THAN K, RUN `addFilterSharingEdges`
						matchCount := countTargetFilterEdges(node, nodeAllowList)
						if matchCount < 5 {
							fmt.Printf("\nBEFORE INTERVENTION: NodeId: %d, has %f filter sharing neighbors.", node.id, matchCount)
							nodeVec := IDsToVector[node.id]
							mutexForGraphRepair.Lock()
							vectorIndex.addFilterTargetEdges(nodeVec, node, nodeAllowList, 128) // Grid Search on this
							mutexForGraphRepair.Unlock()
							matchCount := countTargetFilterEdges(node, nodeAllowList)
							fmt.Printf("\nAFTER INTERVENTION: NodeId: %d, now has %f filter sharing neighbors.", node.id, matchCount)
						}
						// ADD FILTERS FROM MAJORITY TO MINORITYr
						// Check if this is the majority filter
						// Hard-coded, but logic to get the majority filter is above
						if val, ok := nodeFilter[0]; !ok || val == 0 {
							// Majority node, connect to minority nodes
							// Before Intervention Log
							minorityAllowList := buildAllowList(minorityFilter, filterToIDs)
							matchCount := countTargetFilterEdges(node, minorityAllowList)
							if matchCount == 0 {
								fmt.Printf("\nBEFORE INTERVENTION: NodeId: %d, has %f minority filter neighbors.", node.id, matchCount)
								nodeVec := IDsToVector[node.id]
								mutexForGraphRepair.Lock()
								vectorIndex.addFilterTargetEdges(nodeVec, node, minorityAllowList, 1)
								mutexForGraphRepair.Unlock()
								matchCount = countTargetFilterEdges(node, minorityAllowList)
								fmt.Printf("\nAFTER INTERVENTION: NodeId: %d, has %f minority filter neighbors.", node.id, matchCount)
							}
						}
					}
				}(workerID, jobs)
			}
			wgForGraphRepair.Wait()
			fmt.Printf("Graph Repair took %s \n", time.Since(addEdgesTimer))
		}
		/* TEST RECALL AND LATENCY */
		/* SET K */
		k := 100
		/* SET DATA STRUCTURES */
		var relevant_retrieved int
		RecallPerFilter := make(map[int]map[int][]float32)
		LatenciesPerFilter := make(map[int]map[int][]float32)
		// Init Latencies Per Filter
		// ToDo - Derive the filters from somewhere else rather than hardcoding them.
		allFilters := []map[int]int{{0: 0}, {0: 1}}
		for _, filterMap := range allFilters {
			for outerFilter, innerFilter := range filterMap {
				if _, exists := LatenciesPerFilter[outerFilter]; !exists {
					LatenciesPerFilter[outerFilter] = make(map[int][]float32)
					RecallPerFilter[outerFilter] = make(map[int][]float32)
				}
				LatenciesPerFilter[outerFilter][innerFilter] = []float32{}
				RecallPerFilter[outerFilter][innerFilter] = []float32{}
			}
		}
		/* CALCULATE RECALL AND LATENCY */
		var results []uint64
		for i := 0; i < len(queryVectorsWithFilters); i++ {
			queryFilters := queryVectorsWithFilters[i].FilterMap
			allowListIDs := []uint64{}
			for filterKey, filterValue := range queryFilters {
				allowListIDs = append(allowListIDs, filterToIDs[filterKey][filterValue]...)
			}
			queryAllowList := helpers.NewAllowList(allowListIDs...)
			queryStart := time.Now()
			if hnsw_efg {
				results, _, err = vectorIndex.FilteredSearchWithExtendedGraph(queryVectorsWithFilters[i].Vector, k, queryAllowList)
			} else {
				results, _, err = vectorIndex.SearchByVector(queryVectorsWithFilters[i].Vector, k, queryAllowList)
			}
			require.Nil(t, err)
			local_latency := float32(time.Now().Sub(queryStart).Seconds())
			relevant_retrieved = matchesInLists(truths[i].Truths, results)
			/* FOR EXTERME FILTER CASES THERE MAY BE LESS THAN 100 -- REPLACE WITH `len(truths[i].Truths)` */
			local_recall := float32(relevant_retrieved) / 100
			for outerFilter, innerFilter := range queryFilters {
				LatenciesPerFilter[outerFilter][innerFilter] = append(LatenciesPerFilter[outerFilter][innerFilter], local_latency)
				RecallPerFilter[outerFilter][innerFilter] = append(RecallPerFilter[outerFilter][innerFilter], local_recall)
			}
		}
		fmt.Println("Saving LatenciesPerFilter and RecallPerFilter...")
		if hnsw_efg {
			saveJSON("LatenciesPerFilter-intervention.json", LatenciesPerFilter)
			saveJSON("RecallPerFilter-intervention.json", RecallPerFilter)
		} else {
			saveJSON("LatenciesPerFilter.json", LatenciesPerFilter)
			saveJSON("RecallPerFilter.json", RecallPerFilter)
		}
		// Calculate and Report Averages
		printAverageLatencyOrRecall(LatenciesPerFilter, "Latency")
		printAverageLatencyOrRecall(RecallPerFilter, "Recall")

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
			nodeAllowList := buildAllowList(nodeFilter, filterToIDs)
			count := countTargetFilterEdges(node, nodeAllowList)
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
			for filterValue, avgNeighbors := range filterValueMap {
				fmt.Printf("\nFilter [%d:%d] Average Matching Neighbors: %f\n", filterKey, filterValue, avgNeighbors)
			}
		}
	})
}

func buildAllowList(filter map[int]int, filterToIDs map[int]map[int][]uint64) helpers.AllowList {
	AllowListIDs := []uint64{}
	for filterKey, filterValue := range filter {
		AllowListIDs = append(AllowListIDs, filterToIDs[filterKey][filterValue]...)
	}
	AllowList := helpers.NewAllowList(AllowListIDs...)
	return AllowList
}

func countTargetFilterEdges(node *vertex, allowList helpers.AllowList) float32 {
	// COUNT NEIGHBORS ON ALLOWLIST
	var count float32 = 0.0
	for _, connection := range node.connections[0] {
		if allowList.Contains(connection) {
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

func saveJSON(filename string, data interface{}) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Error marshaling data to JSON: %v", err)
	}
	if err := ioutil.WriteFile(filename, jsonData, 0644); err != nil {
		log.Fatalf("Error writing JSON to file: %v", err)
	}
}

func printAverageLatencyOrRecall(data map[int]map[int][]float32, reporting string) {
	for outerFilter, innerMap := range data {
		for innerFilter, values := range innerMap {
			avg := average(values)
			fmt.Printf("Average %s for Filter %d-%d = %f\n", reporting, outerFilter, innerFilter, avg)
		}
	}
}

func average(values []float32) float32 {
	var sum float32
	for _, v := range values {
		sum += v
	}
	return sum / float32(len(values))
}
