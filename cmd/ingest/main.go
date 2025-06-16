package main

import (
	// std
	"flag"
	"log"
	"os"

	// external
	"github.com/PuerkitoBio/goquery"

	// internal
	"github.com/hoodnoah/cod_data_request/internal/datarequest"
)

func main() {
	// define flags
	inputPath := flag.String("input", "", "Path to the HTML file (required)")
	csvDir := flag.String("csv", "", "Directory to write CSV output (optional)")
	parquetDir := flag.String("parquet", "", "Directory to write parquet output (optional)")
	flag.Parse()

	if *inputPath == "" {
		log.Fatal("You must specify --input HTML file path")
	}

	// Open, parse HTML file
	f, err := os.Open(*inputPath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", *inputPath, err)
	}
	defer f.Close()

	doc, err := goquery.NewDocumentFromReader(f)
	if err != nil {
		log.Fatalf("Failed to parse HTML: %v", err)
	}

	datarequest := datarequest.NewCodDataRequest()
	if err := datarequest.ParseHtml(doc); err != nil {
		log.Fatalf("failed to parse cod data request: %v", err)
	}

	if *csvDir != "" {
		if err := datarequest.ToCSV(*csvDir); err != nil {
			log.Fatalf("failed to write records to CSV: %v", err)
		}
		log.Printf("CSV saved to %s\n", *csvDir)
	}

	if *parquetDir != "" {
		if err := datarequest.ToParquet(*parquetDir); err != nil {
			log.Fatalf("failed to write records to parquet: %v", err)
		}
		log.Printf("Parquet saved to %s\n", *parquetDir)
	}
}
