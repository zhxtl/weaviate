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

package common

import (
	"sync"
)

type ShardedLocks interface {
	LockAll()
	UnlockAll()
	LockedAll(callback func())

	RLockAll()
	RUnlockAll()
	RLockedAll(callback func())

	Lock(id uint64)
	Unlock(id uint64)
	Locked(id uint64, callback func())

	RLock(id uint64)
	RUnlock(id uint64)
	RLocked(id uint64, callback func())
}

const DefaultShardedLocksCount = 512

type shardedLocks struct {
	// number of sharded locks
	count int
	// ensures single LockAll and multiple RLockAll / Lock / RLock
	// (LockAll is exclusive to RLockAll / Lock / RLock)
	writeAll *sync.RWMutex
	// ensures RLockAll and Lock are exclusive
	pair pairReadMutex
	// sharded locks
	shards []*sync.RWMutex
}

func NewDefaultShardedLocks() *shardedLocks {
	return NewShardedLocks(DefaultShardedLocksCount)
}

func NewShardedLocks(count int) *shardedLocks {
	if count < 2 {
		count = 2
	}

	writeAll := new(sync.RWMutex)
	pair := newLockBasedPairReadMutex()
	shards := make([]*sync.RWMutex, count)
	for i := 0; i < count; i++ {
		shards[i] = new(sync.RWMutex)
	}

	return &shardedLocks{
		count:    count,
		writeAll: writeAll,
		pair:     pair,
		shards:   shards,
	}
}

func (sl *shardedLocks) LockAll() {
	sl.writeAll.Lock()
}

func (sl *shardedLocks) UnlockAll() {
	sl.writeAll.Unlock()
}

func (sl *shardedLocks) LockedAll(callback func()) {
	sl.LockAll()
	defer sl.UnlockAll()

	callback()
}

func (sl *shardedLocks) Lock(id uint64) {
	sl.writeAll.RLock()
	sl.pair.leftRLock()
	sl.shards[sl.modid(id)].Lock()
}

func (sl *shardedLocks) Unlock(id uint64) {
	sl.shards[sl.modid(id)].Unlock()
	sl.pair.leftRUnlock()
	sl.writeAll.RUnlock()
}

func (sl *shardedLocks) Locked(id uint64, callback func()) {
	sl.Lock(id)
	defer sl.Unlock(id)

	callback()
}

func (sl *shardedLocks) RLockAll() {
	sl.writeAll.RLock()
	sl.pair.rightRLock()
}

func (sl *shardedLocks) RUnlockAll() {
	sl.pair.rightRUnlock()
	sl.writeAll.RUnlock()
}

func (sl *shardedLocks) RLockedAll(callback func()) {
	sl.RLockAll()
	defer sl.RUnlockAll()

	callback()
}

func (sl *shardedLocks) RLock(id uint64) {
	sl.writeAll.RLock()
	sl.shards[sl.modid(id)].RLock()
}

func (sl *shardedLocks) RUnlock(id uint64) {
	sl.shards[sl.modid(id)].RUnlock()
	sl.writeAll.RUnlock()
}

func (sl *shardedLocks) RLocked(id uint64, callback func()) {
	sl.RLock(id)
	defer sl.RUnlock(id)

	callback()
}

func (sl *shardedLocks) modid(id uint64) uint64 {
	return id % uint64(sl.count)
}

// allows multiple left locks or multiple right locks
// (left and right are exclusive)
type pairReadMutex interface {
	leftRLock()
	leftRUnlock()
	rightRLock()
	rightRUnlock()
}

type lockBasedPairReadMutex struct {
	right *sync.RWMutex
	left  *sync.RWMutex
	// allows safe transition between left and right
	change *sync.Mutex
}

func newLockBasedPairReadMutex() *lockBasedPairReadMutex {
	return &lockBasedPairReadMutex{
		right:  new(sync.RWMutex),
		left:   new(sync.RWMutex),
		change: new(sync.Mutex),
	}
}

func (m *lockBasedPairReadMutex) leftRLock() {
	m.aRLock(m.left, m.right)
}

func (m *lockBasedPairReadMutex) leftRUnlock() {
	m.aRUnlock(m.left, m.right)
}

func (m *lockBasedPairReadMutex) rightRLock() {
	m.aRLock(m.right, m.left)
}

func (m *lockBasedPairReadMutex) rightRUnlock() {
	m.aRUnlock(m.right, m.left)
}

func (m *lockBasedPairReadMutex) aRLock(a, b *sync.RWMutex) {
	m.change.Lock()
	// wait until b not locked
	b.Lock()
	a.RLock()
	m.change.Unlock()

	b.Unlock()
}

func (m *lockBasedPairReadMutex) aRUnlock(a, b *sync.RWMutex) {
	a.RUnlock()
}
