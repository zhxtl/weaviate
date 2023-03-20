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

package hnsw

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

const (
	DefaultPQEnabled             = false
	DefaultPQBitCompression      = false
	DefaultPQSegments            = 0
	DefaultPQEncoderType         = "kmeans"
	DefaultPQEncoderDistribution = "log-normal"
	DefaultPQCentroids           = 256
)

// Product Quantization encoder configuration
type PQEncoder struct {
	Type         string `json:"type"`
	Distribution string `json:"distribution,omitempty"`
}

// Product Quantization configuration
type PQConfig struct {
	Enabled        bool      `json:"enabled"`
	BitCompression bool      `json:"bitCompression"`
	Segments       int       `json:"segments"`
	Centroids      int       `json:"centroids"`
	Encoder        PQEncoder `json:"encoder"`
	CodebookUrl    string    `json:"codebookUrl"`
}

func ValidEncoder(encoder string) (ssdhelpers.Encoder, error) {
	switch encoder {
	case "tile":
		return ssdhelpers.UseTileEncoder, nil
	case "kmeans":
		return ssdhelpers.UseKMeansEncoder, nil
	default:
		return 0, fmt.Errorf("invalid encoder type: %s", encoder)
	}
}

func ValidEncoderDistribution(distribution string) (ssdhelpers.EncoderDistribution, error) {
	switch distribution {
	case "log-normal":
		return ssdhelpers.LogNormalEncoderDistribution, nil
	case "normal":
		return ssdhelpers.NormalEncoderDistribution, nil
	default:
		return 0, fmt.Errorf("invalid encoder distribution: %s", distribution)
	}
}

func ValidCodebookPath(codebookUrl string) (*url.URL, error) {
	u, err := url.Parse(codebookUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid codebook url: %s", err)
	}
	if !strings.HasSuffix(codebookUrl, ".json") {
		return nil, fmt.Errorf("only json codebook urls supported")
	}
	if u.Scheme != "file" && u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid codebook url scheme: %s", u.Scheme)
	}
	return u, nil
}

func RetrieveCodebookFromUrl(codebookUrl string) ([][][]float32, error) {
	u, err := ValidCodebookPath(codebookUrl)
	if err != nil {
		return nil, err
	}

	var codebookReader io.Reader

	if u.Scheme == "http" || u.Scheme == "https" {
		ctx := context.Background()
		client := http.Client{
			Timeout: 30 * time.Second,
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, codebookUrl, nil)
		if err != nil {
			return nil, fmt.Errorf("could not download codebook: %s", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("could not download codebook: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("could not download codebook: %s", resp.Status)
		}

		codebookReader = resp.Body
	}

	if u.Scheme == "file" {

		fmt.Printf("Loading codebook from file: %s", u.Path)
		f, err := os.Open(u.Path)
		if err != nil {
			return nil, fmt.Errorf("could not open codebook file: %s", err)
		}
		defer f.Close()

		codebookReader = f
	}

	// numpy codebook format
	// r, err := npyio.NewReader(codebookReader)
	// if err != nil {
	// 	return nil, fmt.Errorf("could not read codebook from file: %s", err)
	// }

	// shape := r.Header.Descr.Shape

	// if len(shape) != 3 {
	// 	return nil, fmt.Errorf("invalid codebook shape: %v, should be of format (segments, centroids, segment length)", shape)
	// }

	// // read initially as contiguous slice and then reshape
	// codebookData := make([]float32, shape[0]*shape[1]*shape[2])
	// err = r.Read(&codebookData)
	// if err != nil {
	// 	return nil, fmt.Errorf("could not read codebook data: %s", err)
	// }

	// codebook := make([][][]float32, shape[0])
	// idx := 0
	// for i := range codebook {
	// 	codebook[i] = make([][]float32, shape[1])
	// 	for j := range codebook[i] {
	// 		codebook[i][j] = codebookData[idx : idx+shape[2]]
	// 		idx += shape[2]
	// 	}
	// }

	content, err := io.ReadAll(codebookReader)
	if err != nil {
		return nil, fmt.Errorf("could read codebook file: %s", err)
	}

	// Now let's unmarshall the data into a var
	var codebook [][][]float32
	err = json.Unmarshal(content, &codebook)
	if err != nil {
		return nil, fmt.Errorf("could parse codebook json: %s", err)
	}

	return codebook, nil

}

func codebookUrlFromMap(in map[string]interface{}, setFn func(v string)) error {
	value, ok := in["codebookUrl"]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return nil
	}

	_, err := ValidCodebookPath(asString)
	if err != nil {
		return err
	}

	setFn(asString)
	return nil
}

func encoderFromMap(in map[string]interface{}, setFn func(v string)) error {
	value, ok := in["type"]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return nil
	}

	_, err := ValidEncoder(asString)
	if err != nil {
		return err
	}

	setFn(asString)
	return nil
}

func encoderDistributionFromMap(in map[string]interface{}, setFn func(v string)) error {
	value, ok := in["distribution"]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return nil
	}

	_, err := ValidEncoderDistribution(asString)
	if err != nil {
		return err
	}

	setFn(asString)
	return nil
}

func parsePQMap(in map[string]interface{}, pq *PQConfig) error {
	pqConfigValue, ok := in["pq"]
	if !ok {
		return nil
	}

	pqConfigMap, ok := pqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := optionalBoolFromMap(pqConfigMap, "enabled", func(v bool) {
		pq.Enabled = v
	}); err != nil {
		return err
	}

	if err := optionalBoolFromMap(pqConfigMap, "bitCompression", func(v bool) {
		pq.BitCompression = v
	}); err != nil {
		return err
	}

	if err := optionalIntFromMap(pqConfigMap, "segments", func(v int) {
		pq.Segments = v
	}); err != nil {
		return err
	}

	if err := codebookUrlFromMap(pqConfigMap, func(v string) {
		pq.CodebookUrl = v
	}); err != nil {
		return err
	}

	if err := optionalIntFromMap(pqConfigMap, "centroids", func(v int) {
		pq.Centroids = v
	}); err != nil {
		return err
	}

	pqEncoderValue, ok := pqConfigMap["encoder"]
	if !ok {
		return nil
	}

	pqEncoderMap, ok := pqEncoderValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := encoderFromMap(pqEncoderMap, func(v string) {
		pq.Encoder.Type = v
	}); err != nil {
		return err
	}

	if err := encoderDistributionFromMap(pqEncoderMap, func(v string) {
		pq.Encoder.Distribution = v
	}); err != nil {
		return err
	}

	return nil
}
