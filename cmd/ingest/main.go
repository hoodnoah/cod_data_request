package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/PuerkitoBio/goquery"

	"github.com/hoodnoah/cod_data_request/internal/types"
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

	records, err := types.FromHtml(doc)
	if err != nil {
		log.Fatalf("Failed to parse checkpoint records: %v", err)
	}

	if *csvDir != "" {
		outputPath := filepath.Join(*csvDir, "black_ops_6_campaign_checkpoints.csv")
		if err := types.ToCSV(outputPath, records); err != nil {
			log.Fatalf("Failed to write CSV: %v", err)
		}
		log.Printf("CSV saved to %s\n", outputPath)
	}

	if *parquetDir != "" {
		outputPath := filepath.Join(*parquetDir, "black_ops_6_campaign_checkpoints.parquet")
		if err := types.ToParquet(outputPath, records); err != nil {
			log.Fatalf("Failed to write parquet: %v", err)
		}
		log.Printf("Parquet saved to %s\n", outputPath)
	}
}
