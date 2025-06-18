package helpers

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type FieldParser func(string) (any, error)

func StringParser() FieldParser {
	return func(s string) (any, error) {
		return s, nil
	}
}

func IntParser() FieldParser {
	return func(s string) (any, error) {
		return tryParseInt(s)
	}
}

func FloatParser() FieldParser {
	return func(s string) (any, error) {
		return tryParseFloat(s)
	}
}

func TimeParser() FieldParser {
	return func(s string) (any, error) {
		return tryParseTimeUTC(s)
	}
}

func TimestampToUnixMillisInt64() FieldParser {
	return func(s string) (any, error) {
		ts, err := tryParseTimeUTC(s)
		if err != nil {
			return 0, err
		}
		return ts.UnixMilli(), nil
	}
}

func tryParseTimeUTC(ts string) (time.Time, error) {
	t, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(ts))
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func tryParseFloat(ts string) (float64, error) {
	// trim off a percentage
	tsClean := strings.TrimSuffix(ts, "%")

	t, err := strconv.ParseFloat(tsClean, 32)
	if err != nil {
		return 0.0, err
	}

	return math.Round(t*100) / 100, nil
}

func tryParseInt(ts string) (int64, error) {
	i, err := strconv.ParseInt(ts, 10, 32)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func MakeFromRow[T any](tagName string, parsers map[string]FieldParser) func([]string, []string) (*T, error) {
	return func(header, row []string) (*T, error) {
		if len(header) == 0 {
			return nil, errors.New("expected a header with cells, received none")
		}

		if len(row) == 0 {
			return nil, errors.New("expected a row with cells, received none")
		}

		if len(header) != len(row) {
			return nil, fmt.Errorf("expected header (len %d) to equal row (len %d) length", len(header), len(row))
		}

		return ParseRowReflect[T](header, row, tagName, parsers)
	}
}

func FromHtmlTable[T any](
	doc *goquery.Document,
	h1Text, h2Text string,
	fromRow func([]string, []string) (*T, error),
) ([]*T, error) {
	header, rows, err := FindTable(doc, h1Text, h2Text)
	if err != nil {
		return nil, err
	}
	if len(header) == 0 {
		return nil, errors.New("header row not found")
	}
	if len(rows) == 0 {
		return nil, errors.New("no rows found")
	}

	var result []*T
	for i, row := range rows {
		res, err := fromRow(header, row)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
		if res == nil {
			return nil, fmt.Errorf("row %d: nul result", i+1)
		}
		result = append(result, res)
	}
	return result, nil
}

func ParseRowReflect[T any](header []string, row []string, tagName string, fieldParsers map[string]FieldParser) (*T, error) {
	if len(header) != len(row) {
		return nil, fmt.Errorf("header was length %d, row was length %d, expected a match", len(header), len(row))
	}

	var result T
	v := reflect.ValueOf(&result).Elem()
	t := v.Type()

	for i, column := range header {
		cell := row[i]

		// Match struct field by tag
		var field reflect.StructField
		var found bool
		for j := range t.NumField() {
			if t.Field(j).Tag.Get(tagName) == column {
				field = t.Field(j)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("field with tag %q not found in struct", column)
		}

		parser, ok := fieldParsers[column]
		if !ok {
			return nil, fmt.Errorf("no parser for column %q", column)
		}

		val, err := parser(cell)
		if err != nil {
			return nil, fmt.Errorf("error parsing field %q: %v", column, err)
		}

		fieldVal := v.FieldByName(field.Name)
		if !fieldVal.CanSet() {
			return nil, fmt.Errorf("cannot set field %s", field.Name)
		}

		valRef := reflect.ValueOf(val)
		if !valRef.Type().ConvertibleTo(fieldVal.Type()) {
			return nil, fmt.Errorf("cannot convert value of type %s to field type %s for column %q", valRef.Type(), fieldVal.Type(), column)
		}
		fieldVal.Set(valRef.Convert(fieldVal.Type()))
	}
	return &result, nil
}
