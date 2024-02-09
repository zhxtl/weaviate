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

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// const maximumNumberOfGoroutines = 10
type ReRankerAllClient interface {
	RankAll(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankAllResult, error)
}

type ReRankerAllProvider struct {
	client ReRankerAllClient
}

func New(rerankerall ReRankerAllClient) *ReRankerAllProvider {
	return &ReRankerAllProvider{rerankerall}
}

func (p *ReRankerAllProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *ReRankerAllProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return p.parseReRankerAllArguments(param)
}

func (p *ReRankerAllProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalReRankerAllField(classname)
}

func (p *ReRankerAllProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.getRerankedResults(ctx, cfg, in, parameters)
	}
	return nil, errors.New("wrong parameters")
}
