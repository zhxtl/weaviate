package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

// Define the struct for JSON unmarshalling
type EmbeddingEntry struct {
	Embedding []float64 `json:"text-embedding-3-large-1536-embedding"`
}

func main() {
	fmt.Println("Read JSON Vectors...")
	vectors, err := ReadJSONVectors("./DBPedia-OpenAI-1M-1536-JSON")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(len(vectors))
}

func ReadJSONVectors(folderPath string) ([][]float32, err error) {

	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return nil, err
	}

	var vectors [][]float32 // This will store all embeddings from all files

	for file_idx, file := range files {
		fmt.Println("Reading file: ", file_idx)
		if !file.IsDir() {
			filePath := filepath.Join(folderPath, file.Name())

			content, err := ioutil.ReadFile(filePath)
			if err != nil {
				fmt.Printf("Error reading file %s: %v\n", file.Name(), err)
				continue // Skip files that cannot be read
			}

			var embeddings []EmbeddingEntry
			err = json.Unmarshal(content, &embeddings)
			if err != nil {
				fmt.Printf("Error decoding JSON in file %s: %v\n", file.Name(), err)
				continue // Skip files that cannot be unmarshalled
			}

			// Convert and append embeddings
			for _, entry := range embeddings {
				innerSlice := make([]float32, len(entry.Embedding))
				for j, v := range entry.Embedding {
					innerSlice[j] = float32(v)
				}
				vectors = append(vectors, innerSlice)
			}
		}
	}

	fmt.Println(len(vectors))
	return vectors, nil
}
