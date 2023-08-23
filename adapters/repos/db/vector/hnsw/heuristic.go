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

package hnsw

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, i, elem.Dist)
		ids[i] = elem.ID
		i++
	}

	var returnList []priorityqueue.ItemWithIndex

	if h.compressed.Load() {
		vecs := make([][]byte, 0, len(ids))
		for _, id := range ids {
			v, err := h.compressedVectorsCache.get(context.Background(), id)
			if err != nil {
				return err
			}
			vecs = append(vecs, v)
		}

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Index]
			good := true
			for _, item := range returnList {
				peerDist := h.pq.DistanceBetweenCompressedVectors(currVec, vecs[item.Index])

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	} else {

		vecs, errs := h.multiVectorForID(context.TODO(), ids)

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop() // p*
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist // d(p*, p)

			currVec := vecs[curr.Index]
			if err := errs[curr.Index]; err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID)
					continue
				} else {
					// not a typed error, we can recover from, return with err
					return errors.Wrapf(err,
						"unrecoverable error for docID %d", curr.ID)
				}
			}
			good := true
			for _, item := range returnList {
				peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
					vecs[item.Index]) // d(p*, p')

				if peerDist < distToQuery {
					// d(p*, p') < d(p*, p)
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// rewind and return to pool
	returnList = returnList[:0]

	// nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}
func (h *hnsw) filteredRobustPrune(input *priorityqueue.Queue,
	max int, nodeFilters map[int]int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())

	// candidates we will subject to pruning
	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())

	// populate candidates with priorityqueue from the input
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, i, elem.Dist)
		ids[i] = elem.ID
		i++
	}

	// Init returnList
	var returnList []priorityqueue.ItemWithIndex

	if h.compressed.Load() {
		vecs := make([][]byte, 0, len(ids))
		for _, id := range ids {
			v, err := h.compressedVectorsCache.get(context.Background(), id)
			if err != nil {
				return err
			}
			vecs = append(vecs, v)
		}

		// Reuses a priorityqueue from the pools
		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		// While we still have candidates in the closestFirst (candidate) queue
		// AND While we still have availability in the returnList (< max)
		for closestFirst.Len() > 0 && len(returnList) < max {
			// Get closest element in the candidate queue
			curr := closestFirst.Pop()
			// Get the filters of the closest node
			currFilters := h.nodes[curr.ID].filters
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			// distance of the closest node to the query
			distToQuery := curr.Dist

			// hold the vector to check with distance to the peer
			currVec := vecs[curr.Index]
			good := true

			// Ok, so we are looping through the nodes we have currently accepted
			for _, item := range returnList {
				peerDist := h.pq.DistanceBetweenCompressedVectors(currVec, vecs[item.Index])
				peerFilters := h.nodes[item.ID].filters

				if peerDist < distToQuery {
					// uh oh, but let's save if the filters are unique
					query_curr_intersection := computeIntersection(nodeFilters, currFilters)
					// Does the query and curr share common filters?
					if query_curr_intersection != nil {
						// Are these filters unique to the peer node that is closer to the query?
						query_peer_intersection := computeIntersection(nodeFilters, peerFilters)
						if query_peer_intersection != nil {
							// The query and the peer do share a unique filter
							// Are these the same filters shared with the curr node?
							uniqueFilterCheck := computeIntersection(query_curr_intersection, query_peer_intersection)
							if uniqueFilterCheck != nil {
								// Yes, the peer node already has the unique filters shared with curr
								// Prune away!
								good = false
								break
							}
						}
					}
				} else {
					// Failed the distance test and doesn't have shared filters...
					good = false // you're out of here!
					break
				}
			}
			// Survived the pruning heuristic! Welcome to the returnList!
			if good {
				returnList = append(returnList, curr)
			}
		}
	} else {

		vecs, errs := h.multiVectorForID(context.TODO(), ids)

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			returnList = append(returnList, curr)
			currFilters := h.nodes[curr.ID].filters
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Index]
			good := true

			if err := errs[curr.Index]; err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID)
					continue
				} else {
					// not a typed error, we can recover from, return with err
					return errors.Wrapf(err,
						"unrecoverable error for docID %d", curr.ID)
				}
			}

			// Ok, so we are looping through the nodes we have currently accepted
			for _, item := range returnList {
				peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
					vecs[item.Index])
				peerFilters := h.nodes[item.ID].filters

				if peerDist < distToQuery {
					// uh oh, but let's save if the filters are unique
					query_curr_intersection := computeIntersection(nodeFilters, currFilters)
					// Do the query and curr share common filters?
					if query_curr_intersection != nil {
						// Are these filters unique to the peer node that is closer to the query?
						query_peer_intersection := computeIntersection(nodeFilters, peerFilters)
						// Don't waste time computing another intersection if the query and peer don't share common filters
						if query_peer_intersection != nil {
							// The query and the peer do share a unique filter
							// Are these the same filters shared with the curr node?
							uniqueFilterCheck := computeIntersection(query_curr_intersection, query_peer_intersection)
							if uniqueFilterCheck != nil {
								// Yes, the peer node already has the unique filters shared with curr
								good = false // Prune away!
								break
							}
						}
					} else {
						// Failed the distance test and doesn't have shared filters...
						good = false // you're out of here!
						break
					}
				}
			}
			// Survived the pruning heuristic! Welcome to the returnList!
			if good {
				returnList = append(returnList, curr)
			}
		}
	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// rewind and return to pool
	returnList = returnList[:0]

	// nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}

func computeIntersection(a, b map[int]int) map[int]int {
	intersection := make(map[int]int)
	for k := range a {
		if bVal, ok := b[k]; ok {
			if bVal == a[k] {
				intersection[k] = a[k]
			}
		}
	}
	if len(intersection) == 0 {
		return nil
	}
	return intersection
}

func filtersEqual(a, b map[int]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
