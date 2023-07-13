package hnsw

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	vectorSize          = 128
	vectorsPerGoroutine = 10
	parallelGoroutines  = 20
	loops               = 1000
)

var deleted *sync.Map = new(sync.Map)

func idVector(ctx context.Context, id uint64) ([]float32, error) {
	if _, ok := deleted.Load(id); ok {
		return nil, storobj.NewErrNotFoundf(id,
			"no object for doc id, it could have been deleted")
	}
	return vector(id), nil
}

func vector(id uint64) []float32 {
	vector := make([]float32, vectorSize)
	for i := 0; i < vectorSize; i++ {
		vector[i] = float32(id)
	}
	return vector
}

func TestCleanupStress(t *testing.T) {
	cm := cyclemanager.NewMulti(cyclemanager.NewFixedIntervalTicker(50 * time.Millisecond))
	cm.Start()

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      idVector,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cm)

	require.Nil(t, err)
	require.NotNil(t, index)

	for k := 0; k < loops; k++ {
		wg := new(sync.WaitGroup)
		for i := 0; i < parallelGoroutines; i++ {
			wg.Add(1)
			go func(i, k int) {
				first := k*parallelGoroutines*vectorsPerGoroutine + i*vectorsPerGoroutine
				ids := make([]uint64, vectorsPerGoroutine)
				for j := 0; j < vectorsPerGoroutine; j++ {
					id := uint64(first + j)
					ids[j] = id
					fmt.Printf("  ==> adding id [%v]\n", id)
					index.Add(id, vector(id))
				}
				for j := 0; j < vectorsPerGoroutine; j++ {
					id := uint64(first + j)
					deleted.Store(id, struct{}{})
				}
				time.Sleep(25 * time.Millisecond)

				fmt.Printf("  ==> deleting ids [%v]\n", ids)
				index.Delete(ids...)
				wg.Done()
			}(i, k)
		}
		wg.Wait()
	}
}
