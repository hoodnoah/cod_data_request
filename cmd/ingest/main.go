package main

import (
	"fmt"
	"log"
	"os"

	"github.com/PuerkitoBio/goquery"

	"github.com/hoodnoah/cod_data_request/internal/types"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <input.html>", os.Args[0])
	}

	path := os.Args[1]
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", path, err)
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

	for i, r := range records {
		fmt.Printf("[%d] %+v\n", i+1, *r)
	}

	fmt.Printf("\n✅ Parsed %d checkpoint records successfully.\n", len(records))
}
