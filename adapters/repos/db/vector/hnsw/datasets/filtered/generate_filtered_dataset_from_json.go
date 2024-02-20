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

//go:build ignore
// +build ignore

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type EmbeddingEntry struct {
	Embedding []float64 `json:"text-embedding-3-large-1536-embedding"`
}

type Vector struct {
	ID     int       `json:"id"`
	Vector []float32 `json:"vector"`
}

type Filters struct {
	ID        int         `json:"id"`
	FilterMap map[int]int `json:"filterMap"`
}

type VecWithFilters struct {
	ID        int         `json:"id"`
	Vector    []float32   `json:"vector"`
	FilterMap map[int]int `json:"filterMap"`
}

type GroundTruth struct {
	QueryID int   `json:"queryID"`
	Truths  []int `json:"truths"`
}

func main() {
	// CLI flags
	numVectors := flag.Int("numVectors", 100_000, "Number of vectors to process")
	majorityPct := flag.Float64("majorityPct", 95.0, "Minority filter percentage of the dataset")
	//vectorDimension := flag.Int("vectorDim", 128, "Dimension of the vectors")
	//filePath := flag.String("DataPath", "./sift-data/sift_base.fvecs", "Path to the data file")

	flag.Parse()

	numLabels := "2"                                                 // ToDo extend to multiple labels
	majorityPct_str := strconv.FormatFloat(*majorityPct, 'f', 1, 64) // used for save path
	majorityPct_str = strings.ReplaceAll(majorityPct_str, ".", "_")  // prefer e.g. `95_0` save path

	// Read base vectors from file

	all_vectors, err := ReadJSONVectors("./DBPedia-OpenAI-1M-1536-JSON")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(len(all_vectors))
	vectors := all_vectors[:990_000]
	queryVectors := all_vectors[990_000:]

	saveIndexVectors := make([]Vector, len(vectors))
	saveIndexFilters := make([]Filters, len(vectors))
	indexForBruteForce := make([]VecWithFilters, len(vectors))

	majority_pct := *majorityPct / 100.0
	majority_cutoff := int(10_000 * majority_pct)

	fmt.Println("Adding filters to indexed vectors.")
	for jdx, vector := range vectors {
		nodeFilterMap := make(map[int]int)
		// ToDo -- extend to K filters, with a parameterized filter distribution (power-law)
		hash := jdx % 10_000
		if hash < majority_cutoff {
			nodeFilterMap[0] = 0
		} else {
			nodeFilterMap[0] = 1
		}
		indexForBruteForce[jdx] = VecWithFilters{
			ID:        jdx,
			FilterMap: nodeFilterMap,
			Vector:    vector,
		}
		saveIndexVectors[jdx] = Vector{
			ID:     jdx,
			Vector: vector,
		}
		saveIndexFilters[jdx] = Filters{
			ID:        jdx,
			FilterMap: nodeFilterMap,
		}
	}

	saveNumVectors := map[int]string{
		100000:  "100K",
		1000000: "1M",
	}
	saveIndexVectorsJSON, _ := json.Marshal(saveIndexVectors)
	index_save_path := "OpenAI-DBedia-indexVectors-" + saveNumVectors[*numVectors] + ".json"
	ioutil.WriteFile(index_save_path, saveIndexVectorsJSON, 0o644)
	index_with_filters_save_path := "OpenAI-DBedia-indexFilters-" + saveNumVectors[*numVectors] + "-" + numLabels + "-" + majorityPct_str + ".json"
	saveIndexFiltersJSON, _ := json.Marshal(saveIndexFilters)
	ioutil.WriteFile(index_with_filters_save_path, saveIndexFiltersJSON, 0o644)

	// Read the query vectors from files
	//_, queryVectors := ReadVecs(*numVectors, 10_000, 128, "sift")

	saveQueryVectors := make([]Vector, len(queryVectors))
	saveQueryFilters := make([]Filters, len(queryVectors))

	groundTruths := make([]GroundTruth, len(queryVectors))

	fmt.Println("Brute forcing...")
	fmt.Println(len(queryVectors))

	workerCount := runtime.GOMAXPROCS(0)
	jobsForWorker := make([][]VecWithFilters, workerCount)
	for i, queryVector := range queryVectors {
		workerID := i % workerCount
		queryFilters := make(map[int]int)
		queryHash := i % 10_000
		if queryHash < majority_cutoff {
			queryFilters[0] = 0
		} else {
			queryFilters[0] = 1
		}
		saveQueryVectors[i] = Vector{
			ID:     i,
			Vector: queryVector,
		}
		saveQueryFilters[i] = Filters{
			ID:        i,
			FilterMap: queryFilters,
		}
		queryVectorWithFilters := VecWithFilters{i, queryVector, queryFilters}
		jobsForWorker[workerID] = append(jobsForWorker[workerID], queryVectorWithFilters)
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	before := time.Now()
	for workerID, jobs := range jobsForWorker {
		wg.Add(1)
		go func(workerID int, myJobs []VecWithFilters) {
			defer wg.Done()
			for i, vecWithFilters := range myJobs {
				originalIndex := (i * workerCount) + workerID
				fmt.Println(originalIndex)
				nearestNeighbors := calculateNearestNeighborsWithFilters(vecWithFilters.Vector, indexForBruteForce, 100, vecWithFilters.FilterMap)
				newGroundTruthJSON := GroundTruth{
					QueryID: originalIndex,
					Truths:  nearestNeighbors,
				}
				mutex.Lock()
				groundTruths[originalIndex] = newGroundTruthJSON
				mutex.Unlock()
			}
		}(workerID, jobs)
	}
	wg.Wait()
	fmt.Printf("Brute forcing took %s \n", time.Since(before))
	fmt.Printf("Saving...\n")
	saveQueryVectorsJSON, _ := json.Marshal(saveQueryVectors)
	query_vectors_save_path := "OpenAI-DBedia-queryVectors-" + saveNumVectors[*numVectors] + ".json"
	ioutil.WriteFile(query_vectors_save_path, saveQueryVectorsJSON, 0o644)
	query_vectors_with_filters_save_path := "OpenAI-DBedia-queryFilters-" + saveNumVectors[*numVectors] + "-" + numLabels + "-" + majorityPct_str + ".json"
	saveQueryFiltersJSON, _ := json.Marshal(saveQueryFilters)
	ioutil.WriteFile(query_vectors_with_filters_save_path, saveQueryFiltersJSON, 0o644)

	// Save all nearest neighbors to a JSON file
	saveGroundTruthsJSON, _ := json.Marshal(groundTruths)
	ground_truth_save_path := "OpenAI-DBedia-filtered-recall-truths-" + saveNumVectors[*numVectors] + "-" + numLabels + "-" + majorityPct_str + ".json"
	ioutil.WriteFile(ground_truth_save_path, saveGroundTruthsJSON, 0o644)

	fmt.Print("\n Finished.")
}

// Function to calculate nearest neighbors of a vector using brute force
func calculateNearestNeighborsWithFilters(query []float32, indexVectors []VecWithFilters, numNeighbors int, queryFilters map[int]int) []int {
	filteredIndex := make([]VecWithFilters, 0)
	for _, v := range indexVectors {
		matches := true
		for queryFilterKey, queryFilterValue := range queryFilters {
			if value, exists := v.FilterMap[queryFilterKey]; !exists || value != queryFilterValue {
				matches = false
				break
			}
		}
		if matches {
			filteredIndex = append(filteredIndex, v)
		}
	}
	//
	type DistanceIndex struct {
		Distance float32
		Index    int
	}
	distances := make([]DistanceIndex, len(filteredIndex))

	// Compute the distance from the query to each vector
	for i, v := range filteredIndex {
		distances[i] = DistanceIndex{
			Distance: BFeuclideanDistance(query, v.Vector),
			Index:    v.ID,
		}
	}

	// Sort the distances
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].Distance < distances[j].Distance
	})

	// Extract the indices of the numNeighbors nearest neighbors
	neighbors := make([]int, numNeighbors)

	i := 0
	for i < numNeighbors {
		neighbors[i] = distances[i].Index
		i++
	}

	return neighbors
}

// Function to calculate the Euclidean distance between two vectors
func BFeuclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return float32(math.Sqrt(float64(sum)))
}

func ReadJSONVectors(folderPath string) (imported_vectors [][]float32, err error) {

	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return nil, err
	}

	var vectors [][]float32 // This will store all embeddings from all files

	for file_idx, file := range files {
		fmt.Println("Reading file: ", file_idx)
		if !file.IsDir() {
			filePath := filepath.Join(folderPath, file.Name())

			content, err := ioutil.ReadFile(filePath)
			if err != nil {
				fmt.Printf("Error reading file %s: %v\n", file.Name(), err)
				continue // Skip files that cannot be read
			}

			var embeddings []EmbeddingEntry
			err = json.Unmarshal(content, &embeddings)
			if err != nil {
				fmt.Printf("Error decoding JSON in file %s: %v\n", file.Name(), err)
				continue // Skip files that cannot be unmarshalled
			}

			// Convert and append embeddings
			for _, entry := range embeddings {
				innerSlice := make([]float32, len(entry.Embedding))
				for j, v := range entry.Embedding {
					innerSlice[j] = float32(v)
				}
				vectors = append(vectors, innerSlice)
			}
		}
	}

	fmt.Println(len(vectors))
	return vectors, nil
}
