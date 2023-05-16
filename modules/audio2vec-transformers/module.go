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

package modaudio

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/audio2vec-transformers/clients"
	"github.com/weaviate/weaviate/modules/audio2vec-transformers/vectorizer"
)

func New() *AudioModule {
	return &AudioModule{}
}

type AudioModule struct {
	vectorizer      audioVectorizer
	graphqlProvider modulecapabilities.GraphQLArguments
	searcher        modulecapabilities.Searcher
}

type audioVectorizer interface {
	Object(ctx context.Context, object *models.Object, objDiff *moduletools.ObjectDiff,
		settings vectorizer.ClassSettings) error
	VectorizeAudio(ctx context.Context,
		id, audio string) ([]float32, error)
}

func (m *AudioModule) Name() string {
	return "audio2vec-transformers"
}

func (m *AudioModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Audio2Vec
}

func (m *AudioModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initVectorizer(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearAudio(); err != nil {
		return errors.Wrap(err, "init near text")
	}

	return nil
}

func (m *AudioModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	// TODO: proper config management
	uri := os.Getenv("IMAGE_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable IMAGE_INFERENCE_API is not set")
	}

	client := clients.New(uri, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)

	return nil
}

func (m *AudioModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *AudioModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *AudioModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)
