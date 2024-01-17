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

package hashtree

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	version      = 1
	headerLength = 1 + 4 + DigestLength // version height root
)

func (ht *HashTree) Serialize(w io.Writer) (n int64, err error) {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	var hdr [headerLength]byte

	hdrOff := 0

	hdr[hdrOff] = version
	hdrOff++

	binary.BigEndian.PutUint32(hdr[hdrOff:], uint32(ht.height))
	hdrOff += 4

	root := ht.root()
	rootBs, err := root.MarshalBinary()
	if err != nil {
		return 0, err
	}
	copy(hdr[hdrOff:], rootBs)

	n1, err := w.Write(hdr[:])
	if err != nil {
		return int64(n1), err
	}

	n = int64(n1)

	// Write leaves
	for i := ht.innerNodesCount; i < len(ht.nodes); i++ {
		nodeBs, err := ht.nodes[i].MarshalBinary()
		if err != nil {
			return n, err
		}

		ni, err := w.Write(nodeBs)
		if err != nil {
			return n + int64(ni), err
		}

		n += int64(ni)
	}

	return n, err
}

func DeserializeHashTree(r io.Reader) (*HashTree, error) {
	var hdr [headerLength]byte

	_, err := r.Read(hdr[:])
	if err != nil {
		return nil, err
	}

	hdrOff := 0

	if hdr[hdrOff] != version {
		return nil, fmt.Errorf("unsupported version %d, expected version %d", hdr[0], version)
	}
	hdrOff++

	height := int(binary.BigEndian.Uint32(hdr[hdrOff:]))
	hdrOff += 4

	var root Digest
	root.UnmarshalBinary(hdr[hdrOff : hdrOff+DigestLength])

	ht := NewHashTree(height)

	for i := 0; i < LeavesCount(ht.Height()); i++ {
		var leafBs [DigestLength]byte

		_, err := r.Read(leafBs[:])
		if err != nil {
			return nil, fmt.Errorf("reading leaf %d: %w", i, err)
		}

		leafPos := ht.innerNodesCount + i
		ht.nodes[leafPos].UnmarshalBinary(leafBs[:])
	}

	if root != ht.Root() {
		return nil, fmt.Errorf("root digest mismatch")
	}

	return ht, nil
}
