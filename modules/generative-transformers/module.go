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

package modgenerativetransformers

import (
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativeadditional "github.com/weaviate/weaviate/modules/generative-transformers/additional"
	generativeadditionalgenerate "github.com/weaviate/weaviate/modules/generative-transformers/additional/generate"
	"github.com/weaviate/weaviate/modules/generative-transformers/clients"
	"github.com/weaviate/weaviate/modules/generative-transformers/ent"
	"net/http"
	"os"
)

const Name = "generative-transformers"

func New() *GenerativeTransformersModule {
	return &GenerativeTransformersModule{}
}

type GenerativeTransformersModule struct {
	generative                   generativeClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type generativeClient interface {
	GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error)
	GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error)
	Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*ent.GenerateResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *GenerativeTransformersModule) Name() string {
	return Name
}

func (m *GenerativeTransformersModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextGenerative
}

func (m *GenerativeTransformersModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init q/a")
	}

	return nil
}

func (m *GenerativeTransformersModule) initAdditional(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	uri := os.Getenv("GENERATIVETRANSFORMERS_INFERENCE_API")
	if uri == "" {
		return nil
	}

	client := clients.New(uri, logger)

	m.generative = client

	generateProvider := generativeadditionalgenerate.New(m.generative)
	m.additionalPropertiesProvider = generativeadditional.New(generateProvider)

	return nil
}

func (m *GenerativeTransformersModule) MetaInfo() (map[string]interface{}, error) {
	return m.generative.MetaInfo()
}

func (m *GenerativeTransformersModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GenerativeTransformersModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
)
