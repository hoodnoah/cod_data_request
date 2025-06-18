package types

type CSVExportable interface {
	ToStringSlice() []string
}

type ParquetExportable interface {
}

type Exportables[T any] []T
