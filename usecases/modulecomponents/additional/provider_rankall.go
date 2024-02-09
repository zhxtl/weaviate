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

package additional

import (
	"context"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

type rerankerAllClient interface {
	RerankerAll(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
}

type GraphQLAdditionalRerankerAllProvider struct {
	ReRankerAllProvider AdditionalProperty
}

func NewRerankerAllProvider(client rerankerAllClient) *GraphQLAdditionalRerankerAllProvider {
	return &GraphQLAdditionalReRankerAllProvider{rerankall.New(client)}
}

func (p *GraphQLAdditionalRerankerAllProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["rerankAll"] = p.getRerankedResults()
	return additionalProperties
}

func (p *GraphQLAdditionalRerankerAllProvider) getReranker() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{"rerankAll"},
		GraphQLFieldFunction:   p.ReRankerAllProvider.AdditionalFieldFn,
		GraphQLExtractFunction: p.ReRankerAllProvider.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.ReRankerAllProvider.AdditionalPropertyFn,
			ExploreList: p.ReRankerAllProvider.AdditionalPropertyFn,
		},
	}
}
