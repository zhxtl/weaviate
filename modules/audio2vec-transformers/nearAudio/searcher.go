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

package nearAudio

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type Searcher struct {
	vectorizer audioVectorizer
}

func NewSearcher(vectorizer audioVectorizer) *Searcher {
	return &Searcher{vectorizer}
}

type audioVectorizer interface {
	VectorizeAudio(ctx context.Context,
		id, audio string) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearAudio"] = s.vectorForNearAudioParam
	return vectorSearches
}

func (s *Searcher) vectorForNearAudioParam(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearAudioParam(ctx, params.(*NearAudioParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearAudioParam(ctx context.Context,
	params *NearAudioParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	searchID := fmt.Sprintf("search_%v", time.Now().UnixNano())
	vector, err := s.vectorizer.VectorizeAudio(ctx, searchID, params.Audio)
	if err != nil {
		return nil, errors.Errorf("vectorize image: %v", err)
	}

	return vector, nil
}
