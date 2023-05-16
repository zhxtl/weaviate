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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)



const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace       = Strategy("replace")
	StrategySetCollection = Strategy("setcollection")
	StrategyMapCollection = Strategy("mapcollection")
	StrategyRoaringSet    = Strategy("roaringset")
)

type Strategy string


func SegmentStrategyFromString(in Strategy) segmentindex.Strategy {
	switch in {
	case StrategyReplace:
		return segmentindex.StrategyReplace
	case StrategySetCollection:
		return segmentindex.StrategySetCollection
	case StrategyMapCollection:
		return segmentindex.StrategyMapCollection
	case StrategyRoaringSet:
		return segmentindex.StrategyRoaringSet
	default:
		panic("unsupported strategy")
	}
}

func IsExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) bool {
	if len(expectedStrategies) == 0 {
		expectedStrategies = []Strategy{StrategyReplace, StrategySetCollection, StrategyMapCollection, StrategyRoaringSet}
	}

	for _, s := range expectedStrategies {
		if s == strategy {
			return true
		}
	}
	return false
}

func CheckExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) {
	if !IsExpectedStrategy(strategy, expectedStrategies...) {
		panic(fmt.Sprintf("one of strategies %v expected, strategy '%s' found", expectedStrategies, strategy))
	}
}
