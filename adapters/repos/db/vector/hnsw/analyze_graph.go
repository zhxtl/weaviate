package hnsw

import (
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func (h *hnsw) analyzeGraph(allowList helpers.AllowList) error {
	allow := allowList.Expose().Clone()

	maxClusters := 0
	for allow.GetCardinality() > 0 {
		if maxClusters > 9 {
			break
		}
		entryPoint, err := allow.Select(0)
		if err != nil {
			return err
		}

		visited, err := h.analyzeLocalCluster(allow, entryPoint)
		if err != nil {
			return err
		}

		allow.AndNot(visited)
		maxClusters++
	}

	return nil
}

func (h *hnsw) analyzeLocalCluster(allow *sroar.Bitmap, entryPoint uint64) (*sroar.Bitmap, error) {
	visited := sroar.NewBitmap()
	current := entryPoint
	candidates := []uint64{entryPoint}
	candidatePointer := 0

	for {
		if candidatePointer == len(candidates) {
			// no more candidates to evaluate, cluster is exhausted
			break
		}
		current = candidates[candidatePointer]
		visited.Set(current)
		candidatePointer++

		n := h.nodes[current]
		if n == nil {
			continue
		}

		for _, conn := range n.connections[0] {
			if !allow.Contains(conn) {
				continue
			}

			if visited.Contains(conn) {
				continue
			}

			candidates = append(candidates, conn)
		}
	}

	fmt.Printf("analyzed cluster with ep %d\n", entryPoint)
	fmt.Printf("filter size: %v\n", len(allow.ToArray()))
	fmt.Printf("visited: %v (%f%%)\n", len(visited.ToArray()), float64(visited.GetCardinality())/float64(allow.GetCardinality()))

	return visited, nil
}
