package lsmkv

import (
	"testing"

	"github.com/weaviate/sroar"
)

func TestCursorPrefixed_RoaringSet(t *testing.T) {

}

func newFakeCursorRoaringSet(size int) *fakeCursorRoaringSet {
	bms := make([]*sroar.Bitmap, size)
	for i := 0; i < size; i++ {
		bm := sroar.NewBitmap()
		bm.Set(uint64(i))
		bms[i] = bm
	}
	return &fakeCursorRoaringSet{counter: 0, bms: }
}

type fakeCursorRoaringSet struct {
	counter int
	bms     []*sroar.Bitmap
}
