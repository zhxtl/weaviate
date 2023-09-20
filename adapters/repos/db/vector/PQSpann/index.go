package pqspann

import (
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const segments = 4
const centroids = 16
const trainingLimit = 100_000
const dimensions = 128
const overlapping = 1

var buckets = int(math.Pow(centroids, segments))
var totalOverlapping = int(math.Pow(overlapping, segments))

var l2 = distancer.NewL2SquaredProvider()

type bucket struct {
	ids     []uint64
	vectors [][]float32
}

type PQSpann struct {
	ids     []uint64
	vectors [][]float32
	pq      *ssdhelpers.ProductQuantizer
	buckets []bucket
}

func NewPQSpann() *PQSpann {
	idx := &PQSpann{
		ids:     make([]uint64, 0, trainingLimit),
		vectors: make([][]float32, 0, trainingLimit),
	}
	return idx
}

func (idx *PQSpann) Add(id uint64, vector []float32) error {
	if len(idx.ids) < trainingLimit {
		idx.ids = append(idx.ids, id)
		idx.vectors = append(idx.vectors, vector)
		return nil
	}

	if idx.pq == nil {
		pq, err := ssdhelpers.NewProductQuantizer(hnsw.PQConfig{
			Enabled:        true,
			BitCompression: false,
			Segments:       segments,
			Centroids:      centroids,
			TrainingLimit:  trainingLimit,
			Encoder: hnsw.PQEncoder{
				Type:         hnsw.PQEncoderTypeKMeans,
				Distribution: hnsw.PQEncoderDistributionNormal,
			},
		}, l2, dimensions)
		if err != nil {
			return err
		}
		idx.pq = pq
		idx.pq.Fit(idx.vectors)

		idx.buckets = make([]bucket, buckets)

		for i := range idx.ids {
			idx.addToBuckets(idx.ids[i], idx.vectors[i])
		}
	}

	idx.addToBuckets(id, vector)
	return nil
}

func (idx *PQSpann) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	return idx.bruteForce(idx.buckets[idx.bucketFor(vector)], vector, k)
}

func (idx *PQSpann) addToBuckets(id uint64, vector []float32) {
	indices := idx.bucketsFor(vector)
	for _, index := range indices {
		idx.buckets[index].ids = append(idx.buckets[index].ids, id)
		idx.buckets[index].vectors = append(idx.buckets[index].vectors, vector)
	}
}

func (idx *PQSpann) bucketFor(vector []float32) int {
	encoded := idx.pq.Encode(vector)
	index := 0
	current := 1
	for j := range encoded {
		index += current * int(encoded[j])
		current *= centroids
	}
	return index
}

func iterateOverIndices(codes [][]int, indices []int, segment int, is []int, index int) int {
	if segment < segments {
		for s := 0; s < overlapping; s++ {
			is[segment] = s
			index = iterateOverIndices(codes, indices, segment+1, is, index)
		}
		return index
	}
	value := 0
	current := 1
	for j := range is {
		value += current * codes[j][is[j]]
		current *= centroids
	}
	indices[index] = value
	return index + 1
}

func (idx *PQSpann) bucketsFor(vector []float32) []int {
	encoded := idx.pq.MultiEncode(vector, overlapping)
	indices := make([]int, totalOverlapping)
	iterateOverIndices(encoded, indices, 0, make([]int, segments), 0)
	return indices
}

func (idx *PQSpann) bruteForce(bucket bucket, vector []float32, k int) ([]uint64, []float32, error) {
	max := priorityqueue.NewMax(k)
	for i := range bucket.ids {
		dist, _, _ := l2.SingleDist(vector, bucket.vectors[i])
		if max.Len() < k || max.Top().Dist > dist {
			max.Insert(bucket.ids[i], dist)
			for max.Len() > k {
				max.Pop()
			}
		}
	}

	ids := make([]uint64, max.Len())
	dists := make([]float32, max.Len())
	for i := max.Len() - 1; i >= 0; i-- {
		elem := max.Pop()
		ids[i] = elem.ID
		dists[i] = elem.Dist
	}
	return ids, dists, nil
}
