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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-gradient/ent"
)

func buildURL(apiEndoint, slug string) string {
	urlTemplate := "https://%s/api/embeddings/%s"
	return fmt.Sprintf(urlTemplate, apiEndoint, slug)
}

type gradient struct {
	apiKey       string
	httpClient   *http.Client
	urlBuilderFn func(apiEndoint, slug string) string
	logger       logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *gradient {
	return &gradient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilderFn: buildURL,
		logger:       logger,
	}
}

func (v *gradient) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *gradient) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *gradient) vectorize(ctx context.Context, input []string, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	endpointURL := v.urlBuilderFn(v.getApiEndpoint(config), v.getSlug(config))
	workspaceId := v.getWorkspaceId(config)
	body, err := json.Marshal(embeddingsRequest{
		Input: input[0],
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Gradient API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("x-gradient-workspace-id", fmt.Sprintf("%s", workspaceId))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("connection to Gradient failed with status: %d", res.StatusCode)
	}

	if len(resBody.Embeddings.Embedding) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &ent.VectorizationResult{
		Text:       input[0],
		Dimensions: len(resBody.Embeddings.Embedding),
		Vector:     resBody.Embeddings.Embedding,
	}, nil
}

func (v *gradient) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	key := "X-Gradient-Api-Key"
	apiKey := ctx.Value(key)
	// try getting header from GRPC if not successful
	if apiKey == nil {
		apiKey = modulecomponents.GetApiKeyFromGRPC(ctx, key)
	}
	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0], nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Gradient-Api-Key " +
		"nor in environment variable under GRADIENT_APIKEY")
}

func (v *gradient) getApiEndpoint(config ent.VectorizationConfig) string {
	return config.ApiEndpoint
}

func (v *gradient) getSlug(config ent.VectorizationConfig) string {
	return config.Slug
}

func (v *gradient) getWorkspaceId(config ent.VectorizationConfig) string {
	return config.WorkspaceId
}

type embeddingsRequest struct {
	Input string `json:"input,omitempty"`
}

type embeddingsResponse struct {
	Embeddings embedding `json:"embeddings,omitempty"`
}

type embedding struct {
	Embedding []float32 `json:"embedding,omitempty"`
}
