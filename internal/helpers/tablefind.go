package helpers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// Finds and returns an H1 element, identified by its text.
// *not* case-sensitive
func findH1Element(doc *goquery.Document, headerText string) (*goquery.Selection, error) {
	var result *goquery.Selection
	doc.Find("h1").EachWithBreak(func(i int, s *goquery.Selection) bool {
		actualHeader := strings.TrimSpace(s.Text())
		if strings.EqualFold(actualHeader, strings.TrimSpace(headerText)) {
			result = s
			return false // stop iteration
		}
		return true // keep searching
	})

	if result == nil {
		return nil, fmt.Errorf("failed to find H1 element with text %s", headerText)
	} else {
		return result, nil
	}
}

// Finds and returns an H2 element, identified by its text.
// *not* case-sensitive
func findH2Element(h1 *goquery.Selection, headerText string) (*goquery.Selection, error) {
	sibling := h1.Next()

	for sibling.Length() > 0 {
		if goquery.NodeName(sibling) == "h2" {
			actualHeader := strings.TrimSpace(sibling.Text())
			if strings.EqualFold(actualHeader, strings.TrimSpace(headerText)) {
				return sibling, nil
			}
		}
		sibling = sibling.Next()
	}

	return nil, fmt.Errorf("failed to find H2 element with text %s", headerText)
}

// Finds the first table element following a given H2 element.
func findTableAfterH2(h2 *goquery.Selection) (*goquery.Selection, error) {
	sibling := h2.Next()

	for sibling.Length() > 0 {
		if goquery.NodeName(sibling) == "table" {
			return sibling, nil
		}
		sibling = sibling.Next()
	}

	return nil, errors.New("failed to find table following H2 element")
}

func FindTableAfterHeaders(doc *goquery.Document, h1Text, h2Text string) (*goquery.Selection, error) {
	// Step 1: find the H1
	h1, err := findH1Element(doc, h1Text)
	if err != nil {
		return nil, err
	}

	// Step 2: find the H2
	h2, err := findH2Element(h1, h2Text)
	if err != nil {
		return nil, err
	}

	// Step 3: find the Table
	table, err := findTableAfterH2(h2)
	if err != nil {
		return nil, err
	}

	return table, nil
}

// parseTable takes a table element and returns its header and data rows.
// Assumes the first <tr> contains <th> elements and all subsequent <tr> contain <td> data rows.
func parseTable(table *goquery.Selection) ([]string, [][]string, error) {
	var headers []string
	var rows [][]string

	// Extract headers
	headerRow := table.Find("tr").First()
	headerRow.Find("th").Each(func(_ int, th *goquery.Selection) {
		headers = append(headers, strings.TrimSpace(th.Text()))
	})

	if len(headers) == 0 {
		return nil, nil, errors.New("no headers found in table")
	}

	// Extract data rows (skip header row)
	table.Find("tr").NextAll().Each(func(_ int, tr *goquery.Selection) {
		var row []string
		tr.Find("td").Each(func(_ int, td *goquery.Selection) {
			row = append(row, strings.TrimSpace(td.Text()))
		})
		if len(row) > 0 {
			rows = append(rows, row)
		}
	})

	return headers, rows, nil
}

func FindTable(doc *goquery.Document, h1Text, h2Text string) ([]string, [][]string, error) {
	table, err := FindTableAfterHeaders(doc, h1Text, h2Text)
	if err != nil {
		return nil, nil, err
	}

	return parseTable(table)
}
