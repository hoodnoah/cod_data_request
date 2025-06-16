package helpers

import (
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// FindTableAfterHeader finds the first table after an <h2> that matches the given header text
func FindTableAfterHeader(doc *goquery.Document, headerText string) ([]string, [][]string, error) {
	var headers []string
	var rows [][]string

	found := false
	doc.Find("h1").EachWithBreak(func(i int, s *goquery.Selection) bool {
		actualHeader := strings.TrimSpace(s.Text())
		if strings.EqualFold(actualHeader, strings.TrimSpace(headerText)) {
			table := s.NextAllFiltered("table").First()

			if table.Length() == 0 {
				return true // keep searching
			}

			// extract header
			table.Find("tr").EachWithBreak(func(i int, tr *goquery.Selection) bool {
				tr.Find("th").Each(func(_ int, th *goquery.Selection) {
					headers = append(headers, strings.TrimSpace(th.Text()))
				})
				return false // only do the first row for headers
			})

			// Extract data rows
			table.Find("tr").Each(func(i int, tr *goquery.Selection) {
				if i == 0 {
					return // skip header row
				}

				var row []string
				tr.Find("td").Each(func(_ int, td *goquery.Selection) {
					row = append(row, strings.TrimSpace(td.Text()))
				})
				if len(row) > 0 {
					rows = append(rows, row)
				}
			})
			found = true
			return false // break loop
		}
		return true // keep searching
	})

	if !found {
		return nil, nil, fmt.Errorf("header not found: %q", headerText)
	}
	return headers, rows, nil
}
