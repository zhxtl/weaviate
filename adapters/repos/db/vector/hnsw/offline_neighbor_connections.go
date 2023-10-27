package hnsw

import (
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
	for level := min(n.targetLevel, n.currentMaxLevel); level >= 0; level-- {
		err := n.doAtLevel(level)
		if err != nil {
			return errors.Wrapf(err, "at level %d", level)
		}
	}
	return nil
}

func (n *offlineNeighborFinderConnector) doAtLevel(level int) error {
	before := time.Now()
	if err := n.pickEntrypoint(); err != nil {
		return errors.Wrap(err, "pick entrypoint at level beginning")
	}

	eps := priorityqueue.NewMin(1)
	eps.Insert(n.entryPointID, n.entryPointDist)

	// Not efConstruction -- this needs to be another parameter to the struct
	// For now, hard-coded 42, the meaning of life
	results, err := n.graph.searchLayerByVector(n.nodeVec, eps, 42, level, n.filterAllowList)
	if err != nil {
		return errors.Wrapf(err, "search layer at level %d", level)
	}

	n.graph.insertMetrics.findAndConnectSearch(before)
	before = time.Now()
}
