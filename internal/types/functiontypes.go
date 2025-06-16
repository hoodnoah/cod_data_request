package types

type CSVExportable interface {
	ToStringSlice() []string
}

type ParquetExportable interface {
	ToExport() any
}

type Exportables[T any] []T
