package lsmkv

import (
	"fmt"
	"sync"
)

// callback, returns nothing, response handled inside callback
type memtableOperation func(id int, m *MemtableSingle)

/*


	INTERFACE


*/

// common interface
type Memtable_ interface {
	countStats() *countStats
	put(key, value []byte, opts ...SecondaryKeyOption) error
	getBySecondary(pos int, key []byte) ([]byte, error)
}

/*


	PREVIOUS IMPLEMENTATION


*/

// previous implementation, no goroutines
type MemtableSingle struct {
	path string
}

func NewMemtableSingle(path string) (*MemtableSingle, error) {
	return &MemtableSingle{path: path}, nil
}

func (ms *MemtableSingle) countStats() *countStats {
	// TODO implementation
	return &countStats{}
}

func (ms *MemtableSingle) put(key, value []byte, opts ...SecondaryKeyOption) error {
	// TODO implementation
	return nil
}

func (ms *MemtableSingle) getBySecondary(pos int, key []byte) ([]byte, error) {
	// TODO implementation
	return make([]byte, 0), nil
}

/*


	NEW WORKERS BASED IMPLEMENTATION


*/

// new implementation based on workers
type MemtableMulti struct {
	path              string
	numWorkers        int
	wgWorkers         *sync.WaitGroup
	operationChannels []chan memtableOperation
}

// number of workers given as argument
func NewMemtableMulti(path string, numWorkers int) (*MemtableMulti, error) {
	operationChannels := make([]chan memtableOperation, numWorkers)
	for i := range operationChannels {
		operationChannels[i] = make(chan memtableOperation)
	}

	// create first to fail fast
	memtables := make([]*MemtableSingle, numWorkers)
	for i := range memtables {
		m, err := NewMemtableSingle(path + fmt.Sprintf("_%d", i))
		if err != nil {
			return nil, fmt.Errorf("Error creating memtable: %w", err)
		}
		memtables[i] = m
	}

	// start workers only if no errors
	wgWorkers := new(sync.WaitGroup)
	wgWorkers.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		id := i
		ch := operationChannels[i]
		m := memtables[i]

		go func() {
			defer wgWorkers.Done()

			for operation := range ch {
				operation(id, m)
			}
		}()
	}

	return &MemtableMulti{
		path:              path,
		numWorkers:        numWorkers,
		wgWorkers:         wgWorkers,
		operationChannels: operationChannels,
	}, nil
}

func (mm *MemtableMulti) countStats() *countStats {
	responseChannels := make([]chan *countStats, mm.numWorkers)
	for i := range responseChannels {
		responseChannels[i] = make(chan *countStats)
	}

	mm.callAllWorkers(func(id int, m *MemtableSingle) {
		responseChannels[id] <- m.countStats()
	})

	countStats := &countStats{
		upsertKeys:     make([][]byte, 0),
		tombstonedKeys: make([][]byte, 0),
	}
	// bottleneck?
	// result is pulled from channels sequentially,
	// loop will wait for next responses until channel will be able to return current one
	for _, ch := range responseChannels {
		stats := <-ch
		countStats.upsertKeys = append(countStats.upsertKeys, stats.upsertKeys...)
		countStats.tombstonedKeys = append(countStats.tombstonedKeys, stats.tombstonedKeys...)
	}
	return countStats
}

func (mm *MemtableMulti) put(key, value []byte, opts ...SecondaryKeyOption) error {
	responseChannel := make(chan error)

	mm.callSingleWorker(func(id int, m *MemtableSingle) {
		responseChannel <- m.put(key, value, opts...)
	}, key)

	return <-responseChannel
}

type bytesErrResp struct {
	bytes []byte
	err   error
}

func (mm *MemtableMulti) getBySecondary(pos int, key []byte) ([]byte, error) {
	responseChannels := make([]chan *bytesErrResp, mm.numWorkers)
	for i := range responseChannels {
		responseChannels[i] = make(chan *bytesErrResp)
	}

	mm.callAllWorkers(func(id int, m *MemtableSingle) {
		resp, err := m.getBySecondary(pos, key)
		responseChannels[id] <- &bytesErrResp{bytes: resp, err: err}
	})

	for _, ch := range responseChannels {
		resp := <-ch
		// consolidate final result out of partial ones
		_ = resp
	}
	return make([]byte, 0), nil
}

func (mm *MemtableMulti) callAllWorkers(operation memtableOperation) {
	// bottleneck?
	// operation is pushed to channels sequentially,
	// loop will wait with next elements until channel will be able to accept current one
	for _, ch := range mm.operationChannels {
		ch <- operation
	}
}

func (mm *MemtableMulti) callSingleWorker(operation memtableOperation, key []byte) {
	// replace with deterministic translation key to id (hash etc)
	id := 0

	mm.operationChannels[id] <- operation
}

// to be called inside flush method, at the end
func (mm *MemtableMulti) closeOperationChannels() {
	for _, ch := range mm.operationChannels {
		close(ch)
	}
	mm.wgWorkers.Wait()
}
