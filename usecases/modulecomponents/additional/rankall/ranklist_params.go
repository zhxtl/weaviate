//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rerankall

type Params struct {
	RankPrompt *string
	Properties *[]string
}

func (n Params) GetRankPrompt() string {
	if n.RankPrompt != nil {
		return *n.RankPrompt
	}
	return ""
}

func (n Params) GetProperties() []string {
	if n.Properties != nil {
		return *n.Properties
	}
	return []{""}
}
