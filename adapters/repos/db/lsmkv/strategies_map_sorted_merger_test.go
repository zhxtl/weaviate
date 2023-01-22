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
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SortedMapMerger_RemoveTombstones(t *testing.T) {
	t.Run("single entry, no tombstones", func(t *testing.T) {
		m := newSortedMapMerger()
		input1 := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}

		input := [][]MapPair{input1}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone for unrelated key", func(t *testing.T) {
		m := newSortedMapMerger()
		input1 := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
			{
				Key:       []byte("unrelated"),
				Tombstone: true,
			},
		}

		input := [][]MapPair{input1}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry with tombstone over two segments", func(t *testing.T) {
		m := newSortedMapMerger()
		input := [][]MapPair{
			{
				{
					Key:   []byte("hello"),
					Value: []byte("world"),
				},
			},
			{
				{
					Key:       []byte("hello"),
					Tombstone: true,
				},
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{}
		assert.Equal(t, expected, actual)
	})

	t.Run("multiple segments including updates", func(t *testing.T) {
		m := newSortedMapMerger()
		input := [][]MapPair{
			{
				{
					Key:   []byte("a"),
					Value: []byte("a1"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c1"),
				},
				{
					Key:   []byte("e"),
					Value: []byte("e1"),
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a2"),
				},
				{
					Key:   []byte("b"),
					Value: []byte("b2"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c2"),
				},
			},
			{
				{
					Key:   []byte("b"),
					Value: []byte("b3"),
				},
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b3"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c2"),
			},
			{
				Key:   []byte("e"),
				Value: []byte("e1"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("multiple segments including deletes and re-adds", func(t *testing.T) {
		m := newSortedMapMerger()
		input := [][]MapPair{
			{
				{
					Key:   []byte("a"),
					Value: []byte("a1"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c1"),
				},
				{
					Key:   []byte("e"),
					Value: []byte("e1"),
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a2"),
				},
				{
					Key:       []byte("b"),
					Tombstone: true,
				},
				{
					Key:   []byte("c"),
					Value: []byte("c2"),
				},
			},
			{
				{
					Key:   []byte("b"),
					Value: []byte("b3"),
				},
				{
					Key:       []byte("e"),
					Tombstone: true,
				},
			},
		}

		actual, err := m.do(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b3"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c2"),
			},
		}
		assert.Equal(t, expected, actual)
	})
}

func Test_SortedMapMerger_KeepTombstones(t *testing.T) {
	m := newSortedMapMerger()

	t.Run("multiple segments including updates, deletes in 2nd segment", func(t *testing.T) {
		input := [][]MapPair{
			{
				{
					Key:   []byte("a"),
					Value: []byte("a1"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c1"),
				},
				{
					Key:   []byte("e"),
					Value: []byte("e1"),
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a2"),
				},
				{
					Key:   []byte("b"),
					Value: []byte("b2"),
				},
				{
					Key:       []byte("c"),
					Tombstone: true,
				},
			},
			{
				{
					Key:   []byte("b"),
					Value: []byte("b3"),
				},
			},
		}

		expected := []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b3"),
			},
			{
				Key:       []byte("c"),
				Tombstone: true,
			},
			{
				Key:   []byte("e"),
				Value: []byte("e1"),
			},
		}

		t.Run("without reusable functionality - fresh state", func(t *testing.T) {
			actual, err := m.doKeepTombstones(input)
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})

		t.Run("with reusable functionality - fresh state", func(t *testing.T) {
			m.reset(input)
			actual, err := m.doKeepTombstonesReusable()
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})
	})

	t.Run("inverse order, deletes in 1st segment", func(t *testing.T) {
		input := [][]MapPair{
			{
				{
					Key:   []byte("b"),
					Value: []byte("b3"),
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a2"),
				},
				{
					Key:   []byte("b"),
					Value: []byte("b2"),
				},
				{
					Key:       []byte("c"),
					Tombstone: true,
				},
			},
			{
				{
					Key:   []byte("a"),
					Value: []byte("a1"),
				},
				{
					Key:   []byte("c"),
					Value: []byte("c1"),
				},
				{
					Key:   []byte("e"),
					Value: []byte("e1"),
				},
			},
		}

		expected := []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a1"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b2"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
			{
				Key:   []byte("e"),
				Value: []byte("e1"),
			},
		}

		t.Run("without reusable functionality - fresh state", func(t *testing.T) {
			actual, err := m.doKeepTombstones(input)
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})

		t.Run("with reusable functionality - dirty state", func(t *testing.T) {
			m.reset(input)
			actual, err := m.doKeepTombstonesReusable()
			require.Nil(t, err)

			assert.Equal(t, expected, actual)
		})
	})
}

func BenchmarkMapMerger_SingleList(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	segs := [][]MapPair{
		constructMapPairs(mapPairsCfg{0, 3_000_000, 0.8}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newSortedMapMerger().do(segs)
	}
}

func BenchmarkMapMerger_NoOverlapFewBigLists(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	segs := [][]MapPair{
		constructMapPairs(mapPairsCfg{0, 3_000_000, 0.8}),
		constructMapPairs(mapPairsCfg{3_000_000, 6_000_000, 0.8}),
		constructMapPairs(mapPairsCfg{6_000_000, 9_000_000, 0.8}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newSortedMapMerger().do(segs)
	}
}

func BenchmarkMapMerger_NoOverlapManySmallLists(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	segs := [][]MapPair{}

	for i := uint64(0); i < 9_000_000; i += 100_000 {
		segs = append(segs, constructMapPairs(mapPairsCfg{i, i + 100_000, 0.8}))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newSortedMapMerger().do(segs)
	}
}

func BenchmarkMapMerger_LittleOverlap(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	segs := [][]MapPair{
		constructMapPairs(mapPairsCfg{0, 2_800_000, 0.8}),
		constructMapPairs(mapPairsCfg{3_000_000, 5_800_000, 0.8}),
		constructMapPairs(mapPairsCfg{6_000_000, 9_000_000, 0.8}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newSortedMapMerger().do(segs)
	}
}

func BenchmarkMapMerger_SimilarLists(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	segs := [][]MapPair{
		constructMapPairs(mapPairsCfg{0, 4_000_000, 0.8}),
		constructMapPairs(mapPairsCfg{0, 4_000_000, 0.8}),
		constructMapPairs(mapPairsCfg{0, 4_000_000, 0.8}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newSortedMapMerger().do(segs)
	}
}

func BenchmarkMapMerger_SimilarRangeDifferentDensity(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	segs := [][]MapPair{
		constructMapPairs(mapPairsCfg{0, 6_000_000, 0.8}),
		constructMapPairs(mapPairsCfg{0, 6_000_000, 0.1}),
		constructMapPairs(mapPairsCfg{0, 6_000_000, 0.01}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newSortedMapMerger().do(segs)
	}
}

type mapPairsCfg struct {
	minID           uint64
	maxID           uint64
	chanceContained float64
}

func constructMapPairs(cfg mapPairsCfg) []MapPair {
	out := []MapPair{}

	for i := cfg.minID; i < cfg.maxID; i++ {
		if rand.Float64() < cfg.chanceContained {
			pair := MapPair{
				Key:   make([]byte, 8),
				Value: make([]byte, 12),
			}
			binary.BigEndian.PutUint64(pair.Key, i)
			out = append(out, pair)
		}
	}

	return out
}
