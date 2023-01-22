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
	"testing"

	"github.com/stretchr/testify/assert"
)

func mustEncode(kvs []MapPair) []value {
	res, err := newMapEncoder().DoMulti(kvs)
	if err != nil {
		panic(err)
	}

	return res
}

func Test_MapPair_EncodingBytes(t *testing.T) {
	kv := MapPair{
		Key:   []byte("hello-world-key1"),
		Value: []byte("this is the value ;-)"),
	}

	control, err := kv.Bytes()
	assert.Nil(t, err)

	encoded := make([]byte, kv.Size())
	err = kv.EncodeBytes(encoded)
	assert.Nil(t, err)

	assert.Equal(t, control, encoded)
}
