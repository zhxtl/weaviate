package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func TestCursorPrefixed_RoaringSet(t *testing.T) {
	type testCase struct {
		prefix string
		id     uint64
	}

	testCases := []testCase{
		{
			prefix: "a",
			id:     1,
		},
		{
			prefix: "b",
			id:     2,
		},
		{
			prefix: "c",
			id:     3,
		},
	}

	for _, tc := range testCases {
		cursor := newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte(tc.prefix))

		t.Run("first", func(t *testing.T) {
			key, bm := cursor.First()
			assertEntry(t, key, bm, []byte("201"), tc.id, 201)
		})

		t.Run("nexts", func(t *testing.T) {
			key1, bm1 := cursor.Next()
			assertEntry(t, key1, bm1, []byte("202"), tc.id, 202)
			key2, bm2 := cursor.Next()
			assertEntry(t, key2, bm2, []byte("203"), tc.id, 203)
			key3, bm3 := cursor.Next()
			assertEntry(t, key3, bm3, []byte("401"), tc.id, 401)
			key4, bm4 := cursor.Next()
			assertEntry(t, key4, bm4, []byte("402"), tc.id, 402)
			key5, bm5 := cursor.Next()
			assertEntry(t, key5, bm5, []byte("403"), tc.id, 403)
			key6, bm6 := cursor.Next()
			assertEntryNil(t, key6, bm6)
			key7, bm7 := cursor.Next()
			assertEntryNil(t, key7, bm7)
		})

		t.Run("seeks", func(t *testing.T) {
			key1, bm1 := cursor.Seek([]byte("402"))
			assertEntry(t, key1, bm1, []byte("402"), tc.id, 402)
			key2, bm2 := cursor.Seek([]byte("333"))
			assertEntry(t, key2, bm2, []byte("401"), tc.id, 401)
			key3, bm3 := cursor.Seek([]byte("203"))
			assertEntry(t, key3, bm3, []byte("203"), tc.id, 203)
			key4, bm4 := cursor.Seek([]byte("200"))
			assertEntry(t, key4, bm4, []byte("201"), tc.id, 201)
			key5, bm5 := cursor.Seek([]byte("404"))
			assertEntryNil(t, key5, bm5)
			key6, bm6 := cursor.Seek([]byte("101"))
			assertEntry(t, key6, bm6, []byte("201"), tc.id, 201)
		})

		t.Run("mix", func(t *testing.T) {
			key1, bm1 := cursor.Seek([]byte("401"))
			assertEntry(t, key1, bm1, []byte("401"), tc.id, 401)
			key2, bm2 := cursor.First()
			assertEntry(t, key2, bm2, []byte("201"), tc.id, 201)
			key3, bm3 := cursor.Seek([]byte("666"))
			assertEntryNil(t, key3, bm3)
			key4, bm4 := cursor.Next()
			assertEntry(t, key4, bm4, []byte("202"), tc.id, 202)
			key5, bm5 := cursor.Next()
			assertEntry(t, key5, bm5, []byte("203"), tc.id, 203)
			key6, bm6 := cursor.First()
			assertEntry(t, key6, bm6, []byte("201"), tc.id, 201)
			key7, bm7 := cursor.Seek([]byte("402"))
			assertEntry(t, key7, bm7, []byte("402"), tc.id, 402)
			key8, bm8 := cursor.Next()
			assertEntry(t, key8, bm8, []byte("403"), tc.id, 403)
			key9, bm9 := cursor.Next()
			assertEntryNil(t, key9, bm9)
			key10, bm10 := cursor.Next()
			assertEntryNil(t, key10, bm10)
			key11, bm11 := cursor.First()
			assertEntry(t, key11, bm11, []byte("201"), tc.id, 201)
			key12, bm12 := cursor.Seek([]byte("201"))
			assertEntry(t, key12, bm12, []byte("201"), tc.id, 201)
			key13, bm13 := cursor.Seek([]byte("403"))
			assertEntry(t, key13, bm13, []byte("403"), tc.id, 403)
			key14, bm14 := cursor.Next()
			assertEntryNil(t, key14, bm14)
			key15, bm15 := cursor.Seek([]byte("403"))
			assertEntry(t, key15, bm15, []byte("403"), tc.id, 403)
		})
	}

	for _, tc := range testCases {
		cursor := newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte(tc.prefix))

		t.Run("next fallbacks to first", func(t *testing.T) {
			key1, bm1 := cursor.Next()
			assertEntry(t, key1, bm1, []byte("201"), tc.id, 201)
			key2, bm2 := cursor.Next()
			assertEntry(t, key2, bm2, []byte("202"), tc.id, 202)
		})
	}
}

func assertEntry(t *testing.T, key []byte, bm *sroar.Bitmap, expectedKey []byte, expectedValues ...uint64) {
	require.Equal(t, expectedKey, key)
	require.ElementsMatch(t, expectedValues, bm.ToArray())
}

func assertEntryNil(t *testing.T, key []byte, bm *sroar.Bitmap) {
	require.Nil(t, key)
	require.Nil(t, bm)
}

type entry struct {
	key []byte
	bm  *sroar.Bitmap
}

func newFakeCursorRoaringSet() *fakeCursorRoaringSet {
	entries := []*entry{
		{key: []byte("a201"), bm: roaringset.NewBitmap(1, 201)},
		{key: []byte("a202"), bm: roaringset.NewBitmap(1, 202)},
		{key: []byte("a203"), bm: roaringset.NewBitmap(1, 203)},
		{key: []byte("a401"), bm: roaringset.NewBitmap(1, 401)},
		{key: []byte("a402"), bm: roaringset.NewBitmap(1, 402)},
		{key: []byte("a403"), bm: roaringset.NewBitmap(1, 403)},
		{key: []byte("b201"), bm: roaringset.NewBitmap(2, 201)},
		{key: []byte("b202"), bm: roaringset.NewBitmap(2, 202)},
		{key: []byte("b203"), bm: roaringset.NewBitmap(2, 203)},
		{key: []byte("b401"), bm: roaringset.NewBitmap(2, 401)},
		{key: []byte("b402"), bm: roaringset.NewBitmap(2, 402)},
		{key: []byte("b403"), bm: roaringset.NewBitmap(2, 403)},
		{key: []byte("c201"), bm: roaringset.NewBitmap(3, 201)},
		{key: []byte("c202"), bm: roaringset.NewBitmap(3, 202)},
		{key: []byte("c203"), bm: roaringset.NewBitmap(3, 203)},
		{key: []byte("c401"), bm: roaringset.NewBitmap(3, 401)},
		{key: []byte("c402"), bm: roaringset.NewBitmap(3, 402)},
		{key: []byte("c403"), bm: roaringset.NewBitmap(3, 403)},
	}

	return &fakeCursorRoaringSet{pos: 0, entries: entries}
}

type fakeCursorRoaringSet struct {
	pos     int
	entries []*entry
}

func (c *fakeCursorRoaringSet) First() ([]byte, *sroar.Bitmap) {
	e := c.entries[0]
	c.pos++
	return e.key, e.bm
}

func (c *fakeCursorRoaringSet) Next() ([]byte, *sroar.Bitmap) {
	if c.pos < len(c.entries) {
		e := c.entries[c.pos]
		c.pos++
		return e.key, e.bm
	}
	return nil, nil
}

func (c *fakeCursorRoaringSet) Seek(key []byte) ([]byte, *sroar.Bitmap) {
	for i, e := range c.entries {
		if string(e.key) >= string(key) {
			c.pos = i
			c.pos++
			return e.key, e.bm
		}
	}
	return nil, nil
}

func (c *fakeCursorRoaringSet) Close() {}
