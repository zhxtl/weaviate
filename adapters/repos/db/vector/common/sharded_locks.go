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
	"context"
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
	// pair := newLockBasedPairReadMutex()
	// pair := newLockBasedPairReadMutexAlt()
	// pair := newCounterBasedPairReadMutex()
	pair := newContextBasedPairReadMutex()
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

type lockBasedPairReadMutexAlt struct {
	right *sync.RWMutex
	left  *sync.RWMutex
}

func newLockBasedPairReadMutexAlt() *lockBasedPairReadMutexAlt {
	return &lockBasedPairReadMutexAlt{
		right: new(sync.RWMutex),
		left:  new(sync.RWMutex),
	}
}

func (m *lockBasedPairReadMutexAlt) leftRLock() {
	m.right.Lock()
	m.left.RLock()
	m.right.Unlock()
}

func (m *lockBasedPairReadMutexAlt) leftRUnlock() {
	m.left.RUnlock()
}

func (m *lockBasedPairReadMutexAlt) rightRLock() {
	m.right.RLock()
	// lock and immediate unlock to ensure exclusiveness with left lock
	m.left.Lock()
	m.left.Unlock()
}

func (m *lockBasedPairReadMutexAlt) rightRUnlock() {
	m.right.RUnlock()
}

type counterBasedPairReadMutex struct {
	leftCounter  int
	rightCounter int
	left         *sync.Mutex
	right        *sync.Mutex
	main         *sync.Mutex
}

func newCounterBasedPairReadMutex() *counterBasedPairReadMutex {
	return &counterBasedPairReadMutex{
		left:  new(sync.Mutex),
		right: new(sync.Mutex),
		main:  new(sync.Mutex),
	}
}

func (m *counterBasedPairReadMutex) leftRLock() {
	m.aRLock(m.left, m.right, &m.leftCounter, &m.rightCounter)
}

func (m *counterBasedPairReadMutex) leftRUnlock() {
	m.aRUnlock(m.left, m.right, &m.leftCounter, &m.rightCounter)
}

func (m *counterBasedPairReadMutex) rightRLock() {
	m.aRLock(m.right, m.left, &m.rightCounter, &m.leftCounter)
}

func (m *counterBasedPairReadMutex) rightRUnlock() {
	m.aRUnlock(m.right, m.left, &m.rightCounter, &m.leftCounter)
}

func (m *counterBasedPairReadMutex) aRLock(a, b *sync.Mutex, aCounter, bCounter *int) {
	m.main.Lock()
	if *bCounter == 0 {
		if *aCounter == 0 {
			a.Lock()
		}
		*aCounter++
		m.main.Unlock()
		return
	}
	m.main.Unlock()

	for {
		b.Lock()
		if !m.main.TryLock() {
			b.Unlock()
			continue
		}
		b.Unlock()

		if *bCounter == 0 {
			if *aCounter == 0 {
				a.Lock()
			}
			*aCounter++
			m.main.Unlock()
			return
		}

		panic("counter should be 0?")
	}
}

func (m *counterBasedPairReadMutex) aRUnlock(a, b *sync.Mutex, aCounter, bCounter *int) {
	m.main.Lock()
	defer m.main.Unlock()

	*aCounter--
	if *aCounter == 0 {
		a.Unlock()
	}
}

type contextBasedPairReadMutex struct {
	leftCounter  int
	rightCounter int
	leftCtx      context.Context
	rightCtx     context.Context
	leftCancel   context.CancelFunc
	rightCancel  context.CancelFunc
	main         *sync.Mutex
}

func newContextBasedPairReadMutex() *contextBasedPairReadMutex {
	return &contextBasedPairReadMutex{
		main: new(sync.Mutex),
	}
}

func (m *contextBasedPairReadMutex) leftRLock() {
	for {
		m.main.Lock()
		if m.rightCounter == 0 {
			if m.leftCounter == 0 {
				m.leftCtx, m.leftCancel = context.WithCancel(context.Background())
			}
			m.leftCounter++
			m.main.Unlock()
			return
		}
		rightCtx := m.rightCtx
		m.main.Unlock()

		<-rightCtx.Done()
	}
}

func (m *contextBasedPairReadMutex) leftRUnlock() {
	m.main.Lock()
	defer m.main.Unlock()

	m.leftCounter--
	if m.leftCounter == 0 {
		m.leftCancel()
	}
}

func (m *contextBasedPairReadMutex) rightRLock() {
	for {
		m.main.Lock()
		if m.leftCounter == 0 {
			if m.rightCounter == 0 {
				m.rightCtx, m.rightCancel = context.WithCancel(context.Background())
			}
			m.rightCounter++
			m.main.Unlock()
			return
		}
		leftCtx := m.leftCtx
		m.main.Unlock()

		<-leftCtx.Done()
	}
}

func (m *contextBasedPairReadMutex) rightRUnlock() {
	m.main.Lock()
	defer m.main.Unlock()

	m.rightCounter--
	if m.rightCounter == 0 {
		m.rightCancel()
	}
}
