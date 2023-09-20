package pqspann_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pqspann "github.com/weaviate/weaviate/adapters/repos/db/vector/PQSpann"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := provider.SingleDist(x, y)
		return dist
	}
}

func TestPQSpann(t *testing.T) {
	vectors_size := 1000000
	queries_size := 1000
	dimensions := 128
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 1
	distancer := distancer.NewL2SquaredProvider()
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, distanceWrapper(distancer), "../diskAnn/testdata")

	var relevant uint64
	var retrieved int

	before := time.Now()
	index := pqspann.NewPQSpann()
	for i, v := range vectors {
		index.Add(uint64(i), v)
	}
	fmt.Println("Time to index data: ", time.Since(before))

	var querying time.Duration = 0
	ssdhelpers.Concurrently(uint64(len(queries)), func(i uint64) {
		before := time.Now()
		results, _, _ := index.SearchByVector(queries[i], k, nil)
		querying += time.Since(before)
		retrieved += k
		relevant += testinghelpers.MatchesInLists(truths[i], results)
	})

	recall := float32(relevant) / float32(retrieved)
	latency := float32(querying.Microseconds()) / float32(queries_size)
	fmt.Println(recall, latency)
	assert.True(t, recall > 0.9)
	assert.True(t, latency < 100000)
}
