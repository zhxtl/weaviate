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

package modrerankerllmopenai

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/rerank-llm-openai/clients"
	additionalprovider "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	rerankllmmodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

const Name = "reranker-llm-openai"

func New() *RerankLLMOpenAIModule {
	return &RerankLLMOpenAIModule{}
}

type RerankLLMOpenAIModule struct {
	rerankLLM                   rerankLLMClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type rerankLLMClient interface {
	Rank(ctx context.Context, query string, explain bool, cfg moduletools.ClassConfig) (*ent.LLMRankResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *RerankLLMOpenAIModule) Name() string {
	return Name
}

func (m *RerankLLMOpenAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextLLMReranker
}

func (m *RerankLLMOpenAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init q/a")
	}

	return nil
}

func (m *RerankLLMOpenAIModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	openAIApiKey := os.Getenv("OPENAI_APIKEY")
	openAIOrganization := os.Getenv("OPENAI_ORGANIZATION")
	azureApiKey := os.Getenv("AZURE_APIKEY")

	client := clients.New(openAIApiKey, openAIOrganization, azureApiKey, timeout, logger)

	m.rerankLLM = client

	m.additionalPropertiesProvider = additionalprovider.NewRerankLLMProvider(m.rerankLLM)

	return nil
}

func (m *RerankLLMOpenAIModule) MetaInfo() (map[string]interface{}, error) {
	return m.rerankLLM.MetaInfo()
}

func (m *RerankLLMOpenAIModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *RerankLLMOpenAIModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
