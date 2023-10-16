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
	"fmt"
	"strings"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type CursorRoaringSet interface {
	First() ([]byte, *sroar.Bitmap)
	Next() ([]byte, *sroar.Bitmap)
	Seek([]byte) ([]byte, *sroar.Bitmap)
	Close()
}

func (b *Bucket) CursorRoaringSet() CursorRoaringSet {
	return b.cursorRoaringSet(false)
}

func (b *Bucket) CursorRoaringSetKeyOnly() CursorRoaringSet {
	return b.cursorRoaringSet(true)
}

func (b *Bucket) cursorRoaringSet(keyOnly bool) CursorRoaringSet {
	b.flushLock.RLock()

	// TODO move to helper func
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		panic(fmt.Sprintf("CursorRoaringSet() called on strategy other than '%s'", StrategyRoaringSet))
	}

	innerCursors, unlockSegmentGroup := b.disk.newRoaringSetCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newRoaringSetCursor())
	}
	innerCursors = append(innerCursors, b.active.newRoaringSetCursor())

	// cursors are in order from oldest to newest, with the memtable cursor
	// being at the very top
	return &cursorRoaringSet{
		combinedCursor: roaringset.NewCombinedCursor(innerCursors, keyOnly),
		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
	}
}

type cursorRoaringSet struct {
	combinedCursor *roaringset.CombinedCursor
	unlock         func()
}

func (c *cursorRoaringSet) First() ([]byte, *sroar.Bitmap) {
	return c.combinedCursor.First()
}

func (c *cursorRoaringSet) Next() ([]byte, *sroar.Bitmap) {
	return c.combinedCursor.Next()
}

func (c *cursorRoaringSet) Seek(key []byte) ([]byte, *sroar.Bitmap) {
	return c.combinedCursor.Seek(key)
}

func (c *cursorRoaringSet) Close() {
	c.unlock()
}

func newCursorPrefixedRoaringSet(cursor CursorRoaringSet, prefix []byte) CursorRoaringSet {
	return &cursorPrefixedRoaringSet{cursor: cursor, prefix: prefix, started: false, finished: false}
}

type cursorPrefixedRoaringSet struct {
	cursor   CursorRoaringSet
	prefix   []byte
	started  bool
	finished bool
}

func (c *cursorPrefixedRoaringSet) First() ([]byte, *sroar.Bitmap) {
	return c.seekInternal(c.prefix)
}

func (c *cursorPrefixedRoaringSet) Next() ([]byte, *sroar.Bitmap) {
	// fallback to First if not used before
	if !c.started {
		return c.First()
	}
	if c.finished {
		return nil, nil
	}
	return c.nextInternal()
}

func (c *cursorPrefixedRoaringSet) Close() {
	c.cursor.Close()
}

func (c *cursorPrefixedRoaringSet) Seek(key []byte) ([]byte, *sroar.Bitmap) {
	return c.seekInternal(_addPrefix(c.prefix, key))
}

func (c *cursorPrefixedRoaringSet) nextInternal() ([]byte, *sroar.Bitmap) {
	return c.extractResult(c.cursor.Next())
}

func (c *cursorPrefixedRoaringSet) seekInternal(prefixedKey []byte) ([]byte, *sroar.Bitmap) {
	return c.extractResult(c.cursor.Seek(prefixedKey))
}

func (c *cursorPrefixedRoaringSet) extractResult(foundPrefixedKey []byte, bm *sroar.Bitmap) ([]byte, *sroar.Bitmap) {
	c.started = true
	// nothing found in internal cursor
	if foundPrefixedKey == nil {
		c.finished = true
		return nil, nil
	}
	// something found, remove prefix if matches
	if key, removed := _removePrefix(c.prefix, foundPrefixedKey); removed {
		c.finished = false
		return key, bm
	}
	c.finished = true
	return nil, nil
}

func _addPrefix(prefix, key []byte) []byte {
	pk := make([]byte, 0, len(prefix)+len(key))
	pk = append(pk, prefix...)
	pk = append(pk, key...)
	return pk
}

func _removePrefix(prefix, prefixedKey []byte) ([]byte, bool) {
	if _matchesPrefix(prefix, prefixedKey) {
		return prefixedKey[len(prefix):], true
	}
	return prefixedKey, false
}

func _matchesPrefix(prefix, prefixedKey []byte) bool {
	return strings.HasPrefix(string(prefixedKey), string(prefix))
}
