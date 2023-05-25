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
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-transformers/ent"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

type generativetransformers struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, logger logrus.FieldLogger) *generativetransformers {
	return &generativetransformers{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *generativetransformers) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *generativetransformers) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *generativetransformers) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*ent.GenerateResult, error) {

	body, err := json.Marshal(prompt)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", v.url("/generate"),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		if resBody.Error != nil {
			return nil, errors.Errorf("connection to Cohere API failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to Cohere API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Generations[0].Text

	return &ent.GenerateResult{
		Result: &textResponse,
	}, nil
}

func (v *generativetransformers) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *generativetransformers) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
	all := compile.FindAll([]byte(prompt), -1)
	for _, match := range all {
		originalProperty := string(match)
		replacedProperty := compile.FindStringSubmatch(originalProperty)[1]
		replacedProperty = strings.TrimSpace(replacedProperty)
		value := textProperties[replacedProperty]
		if value == "" {
			return "", errors.Errorf("Following property has empty value: '%v'. Make sure you spell the property name correctly, verify that the property exists and has a value", replacedProperty)
		}
		prompt = strings.ReplaceAll(prompt, originalProperty, value)
	}
	return prompt, nil
}

/* Change to get URI, use Brain to look up how that looks typically */
func (v *generativetransformers) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type generateInput struct {
	Prompt string `json:"prompt"`
}

type generateResponse struct {
	Generations []generation
	Error       *cohereApiError `json:"error,omitempty"`
}

type generation struct {
	Text string `json:"text"`
}

// need to check this
// I think you just get message
type cohereApiError struct {
	Message string `json:"message"`
}
