package helpers

import (
	"encoding/csv"
	"os"

	"github.com/hoodnoah/cod_data_request/internal/types"
)

func ToCSV[T types.CSVExportable](fileName string, header []string, items []T) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(header); err != nil {
		return err
	}

	for _, item := range items {
		if err := writer.Write(item.ToStringSlice()); err != nil {
			return err
		}
	}
	return nil
}
