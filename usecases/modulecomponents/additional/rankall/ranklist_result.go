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

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
)

func (p *ReRankerAllProvider) getRerankedResults(ctx context.Context, cfg moduletools.ClassConfig,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	if len(in) == 0 {
		return nil, nil
	}
	if params == nil {
		return nil, fmt.Errorf("no params provided")
	}

	rankPrompt := params.GetRankPrompt()
	properties := params.GetProperties()

	// check if user parameter values are valid
	if len(properties) == 0 {
		return in, errors.New("no properties provided")
	}

	documents := make([]string, len(in))
	for i := range in { // for each result of the general GraphQL Query
		// get text property
		rankPropertyValue := ""
		schema := in[i].Object().Properties.(map[string]interface{})
		for property, value := range schema {
			if property == rankProperty {
				if valueString, ok := value.(string); ok {
					rankPropertyValue = valueString
				}
			}
		}
		documents[i] = rankPropertyValue
	}

	// rank results
	rerankerResults, err := p.client.RankAll(ctx, query, documents, cfg)
	if err != nil {
		return nil, fmt.Errorf("error ranking: %w", err)
	}
	// Add type check to rerankedResults here

	out := make([]search.Result, len(in))
	for idx, id := range rerankerResults.RankedIDs {
		out[idx] = in[id]
	}
	return out, nil
	// sort the list

}
