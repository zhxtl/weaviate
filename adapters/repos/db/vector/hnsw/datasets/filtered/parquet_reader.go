//go:build ignore
// +build ignore

package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	// Open an local file
	fr, err := local.NewLocalFileReader("train-00000-of-00063.parquet")
	if err != nil {
		log.Fatal("Can't open file", err)
	}
	defer fr.Close()

	// Create a new reader
	pr, err := reader.NewParquetReader(fr, nil, 4) // adjust the number of parallel readers if necessary
	if err != nil {
		log.Fatal("Can't create parquet reader", err)
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	data := make([]YourStruct, num) // Define YourStruct according to the schema of your Parquet file

	if err = pr.Read(&data); err != nil {
		log.Fatal("Can't read parquet records", err)
	}

	log.Println(data)
}
