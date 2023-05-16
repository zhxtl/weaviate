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

package lsmkv

import (
	"time"

	"github.com/pkg/errors"
)

type BucketOptions struct {
	Strategy *Strategy
	MemtableThreshold *uint64
	WalThreshold *uint64
	FlushAfterIdle *time.Duration
	SecondaryIndices *uint16
	LegacyMapSortingBeforeCompaction *bool
	MemtableResizer *memtableSizeAdvisor
	MonitorCount *bool
}

func (b *BucketOptions) Apply(bucket *Bucket) error {
	if b.Strategy != nil {
		bucket.strategy = *b.Strategy
	}

	if b.MemtableThreshold != nil {
		bucket.memtableThreshold = *b.MemtableThreshold
	}

	if b.WalThreshold != nil {
		bucket.walThreshold = *b.WalThreshold
	}

	if b.FlushAfterIdle != nil {
		bucket.flushAfterIdle = *b.FlushAfterIdle
	}

	if b.SecondaryIndices != nil {
		bucket.secondaryIndices = *b.SecondaryIndices
	}

	if b.LegacyMapSortingBeforeCompaction != nil {
		bucket.legacyMapSortingBeforeCompaction = *b.LegacyMapSortingBeforeCompaction
	}

	if b.MemtableResizer != nil {
		bucket.memtableResizer = b.MemtableResizer
	}

	if b.MonitorCount != nil {
		bucket.monitorCount = *b.MonitorCount
	}

	return nil
}

func (b *BucketOptions) WithStrategy(strategy Strategy) error {
	switch strategy {
	case StrategyReplace, StrategyMapCollection, StrategySetCollection, StrategyRoaringSet:
	default:
		return errors.Errorf("unrecognized strategy %q", strategy)
	}
	b.Strategy = &strategy
	return nil
}

func (b *BucketOptions) WithMemtableThreshold(threshold uint64) error {
	b.MemtableThreshold = &threshold
	return nil
}

func (b *BucketOptions) WithWalThreshold(threshold uint64) error {
	b.WalThreshold = &threshold
	return nil
}

func (b *BucketOptions) WithIdleThreshold(threshold time.Duration) error {
	b.FlushAfterIdle = &threshold
	return nil
}

func (b *BucketOptions) WithSecondaryIndices(count uint16) error {
	b.SecondaryIndices = &count
	return nil
}

func (b *BucketOptions) WithLegacyMapSorting() error {
	legacy := true
	b.LegacyMapSortingBeforeCompaction = &legacy
	return nil
}

func (b *BucketOptions) WithDynamicMemtableSizing(
	initialMB, maxMB, minActiveSeconds, maxActiveSeconds int,
) error {
	mb := 1024 * 1024
	cfg := memtableSizeAdvisorCfg{
		initial:     initialMB * mb,
		stepSize:    10 * mb,
		maxSize:     maxMB * mb,
		minDuration: time.Duration(minActiveSeconds) * time.Second,
		maxDuration: time.Duration(maxActiveSeconds) * time.Second,
	}
	b.MemtableResizer = newMemtableSizeAdvisor(cfg)
	return nil
}

func (b *BucketOptions) WithMonitorCount() error {
	if b.Strategy == nil || *b.Strategy != StrategyReplace {
		return errors.Errorf("count monitoring only supported on 'replace' buckets")
	}
	monitor := true
	b.MonitorCount = &monitor
	return nil
}

type secondaryIndexKeys [][]byte

type SecondaryKeyOption func(s secondaryIndexKeys) error

func WithSecondaryKey(pos int, key []byte) SecondaryKeyOption {
	return func(s secondaryIndexKeys) error {
		if pos > len(s) {
			return errors.Errorf("set secondary index %d on an index of length %d",
				pos, len(s))
		}

		s[pos] = key

		return nil
	}
}
