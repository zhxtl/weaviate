package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func TestCursorPrefixed_RoaringSet(t *testing.T) {
	cursorA := newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte("a"))
	cursorB := newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte("b"))
	cursorC := newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte("c"))

	t.Run("gets first", func(t *testing.T) {
		keyA, bmA := cursorA.First()
		assertEntry(t, keyA, bmA, []byte("101"), 1, 101)

		keyB, bmB := cursorB.First()
		assertEntry(t, keyB, bmB, []byte("101"), 2, 101)

		keyC, bmC := cursorC.First()
		assertEntry(t, keyC, bmC, []byte("101"), 3, 101)
	})
}

func assertEntry(t *testing.T, key []byte, bm *sroar.Bitmap, expectedKey []byte, expectedValues ...uint64) {
	require.Equal(t, expectedKey, key)
	require.ElementsMatch(t, expectedValues, bm.ToArray())
}

type entry struct {
	key []byte
	bm  *sroar.Bitmap
}

func newFakeCursorRoaringSet() *fakeCursorRoaringSet {
	entries := []*entry{
		{key: []byte("a101"), bm: roaringset.NewBitmap(1, 101)},
		{key: []byte("a102"), bm: roaringset.NewBitmap(1, 102)},
		{key: []byte("a103"), bm: roaringset.NewBitmap(1, 103)},
		{key: []byte("a301"), bm: roaringset.NewBitmap(1, 301)},
		{key: []byte("a302"), bm: roaringset.NewBitmap(1, 302)},
		{key: []byte("a303"), bm: roaringset.NewBitmap(1, 303)},
		{key: []byte("b101"), bm: roaringset.NewBitmap(2, 101)},
		{key: []byte("b102"), bm: roaringset.NewBitmap(2, 102)},
		{key: []byte("b103"), bm: roaringset.NewBitmap(2, 103)},
		{key: []byte("b301"), bm: roaringset.NewBitmap(2, 301)},
		{key: []byte("b302"), bm: roaringset.NewBitmap(2, 302)},
		{key: []byte("b303"), bm: roaringset.NewBitmap(2, 303)},
		{key: []byte("c101"), bm: roaringset.NewBitmap(3, 101)},
		{key: []byte("c102"), bm: roaringset.NewBitmap(3, 102)},
		{key: []byte("c103"), bm: roaringset.NewBitmap(3, 103)},
		{key: []byte("c301"), bm: roaringset.NewBitmap(3, 301)},
		{key: []byte("c302"), bm: roaringset.NewBitmap(3, 302)},
		{key: []byte("c303"), bm: roaringset.NewBitmap(3, 303)},
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
