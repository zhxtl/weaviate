package hnsw

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

func (h *hnsw) offlineFindAndConnectNeighbors(node *vertex,
	entryPointID uint64, nodeVec []float32, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList, filterAllowList helpers.AllowList) error {
	nfcOffline := newOfflineNeighborFinderConnector(
		h, node, entryPointID, nodeVec, targetLevel, currentMaxLevel, denyList, filterAllowList)
	return nfcOffline.Do()
}

type offlineNeighborFinderConnector struct {
	graph           *hnsw
	node            *vertex
	entryPointID    uint64
	entryPointDist  float32 // what is this used for?
	nodeVec         []float32
	targetLevel     int
	currentMaxLevel int
	denyList        helpers.AllowList
	filterAllowList helpers.AllowList
}

func newOfflineNeighborFinderConnector(graph *hnsw, node *vertex, entryPointID uint64,
	nodeVec []float32, targetLevel int, currentMaxLevel int,
	denyList helpers.AllowList, filterAllowList helpers.AllowList) *offlineNeighborFinderConnector {
	return &offlineNeighborFinderConnector{
		graph:           graph,
		node:            node,
		entryPointID:    entryPointID,
		nodeVec:         nodeVec,
		targetLevel:     targetLevel,
		currentMaxLevel: currentMaxLevel,
		denyList:        denyList,
		filterAllowList: filterAllowList,
	}
}

func (n *offlineNeighborFinderConnector) Do() error {
	// Just doing this on layer 0 for now
	n.doAtLevel()
	return nil
}

func (n *offlineNeighborFinderConnector) doAtLevel() error {
	level := 0
	before := time.Now()
	if err := n.pickEntrypoint(); err != nil {
		return errors.Wrap(err, "pick entrypoint at level beginning")
	}

	eps := priorityqueue.NewMin(1)
	eps.Insert(n.entryPointID, n.entryPointDist)

	// Not efConstruction -- this needs to be another parameter to the struct
	// For now, hard-coded 42, the meaning of life
	results, err := n.graph.searchLayerByVector(n.nodeVec, eps, 128, level, n.filterAllowList)
	if err != nil {
		return errors.Wrapf(err, "search layer at level %d", level)
	}
	n.graph.insertMetrics.findAndConnectSearch(before)
	before = time.Now()

	// Check Duplicate Results
	newNeighbors := make([]uint64, 0, results.Len())
	currentNeighbors := n.node.connections[level]
	for results.Len() > 0 {
		id := results.Pop().ID
		if neighborhoodContains(id, level, currentNeighbors) == false {
			newNeighbors = append(newNeighbors, id)
		}
	}
	n.graph.pools.pqResults.Put(results)

	// Need to understand this better, maybe search with ef < maxConn - add to maxConn - ef or something
	for _, neighborID := range newNeighbors {
		n.connectNeighborAtLevel(neighborID, level)
	}
	n.graph.commitLog.ReplaceLinksAtLevel(n.node.id, level, newNeighbors)

	return nil
}

func neighborhoodContains(id uint64, level int, neighbors []uint64) bool {
	for _, neighborID := range neighbors {
		if id == neighborID {
			return true
		}
	}
	return false
}

func (n *offlineNeighborFinderConnector) connectNeighborAtLevel(neighborID uint64,
	level int,
) error {
	neighbor := n.graph.nodeByID(neighborID)
	if skip := n.skipNeighbor(neighbor); skip {
		return nil
	}

	neighbor.Lock()
	defer neighbor.Unlock()
	if level > neighbor.level {
		// upgrade neighbor level if the level is out of sync due to a delete re-assign
		neighbor.upgradeToLevelNoLock(level)
	}
	currentConnections := neighbor.connectionsAtLevelNoLock(level)

	maximumConnections := n.maximumConnections(level)
	if len(currentConnections) < maximumConnections {
		// we can simply append
		// updatedConnections = append(currentConnections, n.node.id)
		neighbor.appendConnectionAtLevelNoLock(level, n.node.id, maximumConnections)
		if err := n.graph.commitLog.AddLinkAtLevel(neighbor.id, level, n.node.id); err != nil {
			return err
		}
	} else {
		// we need to run the heurisitc

		dist, ok, err := n.graph.distBetweenNodes(n.node.id, neighborID)
		if err != nil {
			return errors.Wrapf(err, "dist between %d and %d", n.node.id, neighborID)
		}

		if !ok {
			// it seems either the node or the neighbor were deleted in the meantime,
			// there is nothing we can do now
			return nil
		}

		candidates := priorityqueue.NewMax(len(currentConnections) + 1)
		candidates.Insert(n.node.id, dist)

		for _, existingConnection := range currentConnections {
			dist, ok, err := n.graph.distBetweenNodes(existingConnection, neighborID)
			if err != nil {
				return errors.Wrapf(err, "dist between %d and %d", existingConnection, neighborID)
			}

			if !ok {
				// was deleted in the meantime
				continue
			}

			candidates.Insert(existingConnection, dist)
		}

		err = n.graph.selectNeighborsHeuristic(candidates, maximumConnections, n.denyList)
		if err != nil {
			return errors.Wrap(err, "connect neighbors")
		}

		neighbor.resetConnectionsAtLevelNoLock(level)
		if err := n.graph.commitLog.ClearLinksAtLevel(neighbor.id, uint16(level)); err != nil {
			return err
		}

		for candidates.Len() > 0 {
			id := candidates.Pop().ID
			neighbor.appendConnectionAtLevelNoLock(level, id, maximumConnections)
			if err := n.graph.commitLog.AddLinkAtLevel(neighbor.id, level, id); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *offlineNeighborFinderConnector) skipNeighbor(neighbor *vertex) bool {
	if neighbor == n.node {
		// don't connect to self
		return true
	}

	if neighbor == nil || n.graph.hasTombstone(neighbor.id) {
		// don't connect to tombstoned nodes. This would only increase the
		// cleanup that needs to be done. Even worse: A tombstoned node can be
		// cleaned up at any time, also while we are connecting to it. So,
		// while the node still exists right now, it might already be nil in
		// the next line, which would lead to a nil-pointer panic.
		return true
	}

	return false
}

func (n *offlineNeighborFinderConnector) maximumConnections(level int) int {
	if level == 0 {
		return n.graph.maximumConnectionsLayerZero
	}

	return n.graph.maximumConnections
}

func (n *offlineNeighborFinderConnector) pickEntrypoint() error {
	// the neighborFinderConnector always has a suggestion for an entrypoint that
	// it got from the outside, most of the times we can use this, but in some
	// cases we can't. To see if we can use it, three conditions need to be met:
	//
	// 1. it needs to exist in the graph, i.e. be not nil
	//
	// 2. it can't be under maintenance
	//
	// 3. we need to be able to obtain a vector for it

	localDeny := n.denyList.DeepCopy()
	candidate := n.entryPointID

	// make sure the loop cannot block forever. In most cases, results should be
	// found within micro to milliseconds, this is just a last resort to handle
	// the unknown somewhat gracefully, for example if there is a bug in the
	// underlying object store and we cannot retrieve the vector in time, etc.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		success, err := n.tryEpCandidate(candidate)
		if err != nil {
			return err
		}

		if success {
			return nil
		}

		// no success so far, we need to keep going and find a better candidate
		// make sure we never visit this candidate again
		localDeny.Insert(candidate)
		// now find a new one

		alternative, _ := n.graph.findNewLocalEntrypoint(localDeny,
			n.graph.currentMaximumLayer, candidate)
		candidate = alternative
	}
}

func (n *offlineNeighborFinderConnector) tryEpCandidate(candidate uint64) (bool, error) {
	node := n.graph.nodeByID(candidate)
	if node == nil {
		return false, nil
	}

	if node.isUnderMaintenance() {
		return false, nil
	}

	dist, ok, err := n.graph.distBetweenNodeAndVec(candidate, n.nodeVec)
	if err != nil {
		// not an error we could recover from - fail!
		return false, errors.Wrapf(err,
			"calculate distance between insert node and entrypoint")
	}
	if !ok {
		return false, nil
	}

	// we were able to calculate a distance, we're good
	n.entryPointDist = dist
	n.entryPointID = candidate
	return true, nil
}
