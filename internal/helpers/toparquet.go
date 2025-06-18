package helpers

import (
	"os"

	"github.com/hoodnoah/cod_data_request/internal/types"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// Generalizes saving to parquet.
// Provided a path, items implementing ToExport, and a schema (the zero-value of the export type)
// write them to parquet.
func ToParquet[T types.ParquetExportable](outputDir string, items []T, schema any) error {
	// create output file
	fw, err := os.Create(outputDir)
	if err != nil {
		return err
	}
	defer fw.Close()

	// create parquet writer
	pw, err := writer.NewParquetWriterFromWriter(fw, schema, 4)
	if err != nil {
		return err
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED

	// Write each record
	for _, item := range items {
		if err := pw.Write(item); err != nil {
			return err
		}
	}

	// Stop writing
	if err := pw.WriteStop(); err != nil {
		return err
	}

	return nil
}
