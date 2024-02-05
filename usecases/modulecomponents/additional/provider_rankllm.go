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
	rerank "github.com/weaviate/weaviate/usecases/modulecomponents/additional/rankllm"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

type rerankerClient interface {
	Rerank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
}

type GraphQLAdditionalRerankerProvider struct {
	RerankerProvider AdditionalProperty
}

func NewRerankerProvider(client rerankerClient) *GraphQLAdditionalRankerProvider {
	return &GraphQLAdditionalRankerProvider{rerank.New(client)}
}

func (p *GraphQLAdditionalRerankerProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["rerank"] = p.getReranker()
	return additionalProperties
}

func (p *GraphQLAdditionalRerankerProvider) getReranker() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{"rerank"},
		GraphQLFieldFunction:   p.RerankerProvider.AdditionalFieldFn,
		GraphQLExtractFunction: p.RerankerProvider.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.RerankerProvider.AdditionalPropertyFn,
			ExploreList: p.RerankerProvider.AdditionalPropertyFn,
		},
	}
}
