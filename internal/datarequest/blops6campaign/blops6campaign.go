package blops6campaign

import (
	// std

	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	// external
	"github.com/PuerkitoBio/goquery"

	// internal
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

type fieldParser func(string) (any, error)

var headerLabels = []string{
	"timestamp_utc",
	"account_type",
	"device_type",
	"difficulty",
	"level_name",
	"checkpoint",
	"checkpoint_duration_s",
	"deaths",
	"fails",
}

var fieldParsers = map[string]fieldParser{
	"UTC Timestamp": func(s string) (any, error) {
		t, err := helpers.TryParseTimeUTC(s)
		if err != nil {
			return nil, err
		}
		return t, nil
	},
	"Account Type": func(s string) (any, error) { return s, nil },
	"Device Type":  func(s string) (any, error) { return s, nil },
	"Difficulty":   func(s string) (any, error) { return s, nil },
	"Level Name":   func(s string) (any, error) { return s, nil },
	"Checkpoint":   func(s string) (any, error) { return s, nil },
	"Checkpoint Duration": func(s string) (any, error) {
		f, err := helpers.TryParseFloat(s)
		if err != nil {
			return 0.0, err
		}
		return time.Duration(f * float64(time.Second)), nil
	},
	"Deaths": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return 0, err
		}
		return i, nil
	},
	"Fails": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return 0, err
		}
		return i, nil
	},
}

type Checkpoint struct {
	Timestamp          time.Time
	AccountType        string
	DeviceType         string
	Difficulty         string
	LevelName          string
	Checkpoint         string
	CheckpointDuration time.Duration
	Deaths             uint
	Fails              uint
}

type Checkpoints []*Checkpoint

type checkpointExport struct {
	Timestamp          int64   `parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	AccountType        string  `parquet:"name=account_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType         string  `parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Difficulty         string  `parquet:"name=difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LevelName          string  `parquet:"name=level_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Checkpoint         string  `parquet:"name=checkpoint, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CheckpointDuration float32 `parquet:"name=checkpoint_duration_s, type=FLOAT"`
	Deaths             int32   `parquet:"name=deaths, type=INT32"`
	Fails              int32   `parquet:"name=fails, type=INT32"`
}

func (b *Checkpoint) ToExport() any {
	return &checkpointExport{
		Timestamp:          b.Timestamp.UnixMilli(),
		AccountType:        b.AccountType,
		DeviceType:         b.DeviceType,
		Difficulty:         b.Difficulty,
		LevelName:          b.LevelName,
		Checkpoint:         b.Checkpoint,
		CheckpointDuration: float32(b.CheckpointDuration.Seconds()),
		Deaths:             int32(b.Deaths),
		Fails:              int32(b.Fails),
	}
}

func (b *Checkpoint) ToStringSlice() []string {
	return []string{
		b.Timestamp.UTC().Format("2006-01-02 15:04:05"),
		b.AccountType,
		b.DeviceType,
		b.Difficulty,
		b.LevelName,
		b.Checkpoint,
		strconv.FormatInt(int64(b.CheckpointDuration.Seconds()), 10),
		strconv.FormatUint(uint64(b.Deaths), 10),
		strconv.FormatUint(uint64(b.Fails), 10),
	}
}

// parses a BlackOps6CampaignCheckpoint from a header and its rows
func fromRow(header []string, row []string) (*Checkpoint, error) {
	if len(row) == 0 {
		return nil, errors.New("no rows to parse")
	}

	if len(row) != len(header) {
		return nil, fmt.Errorf("row/header length mismatch: %d vs %d", len(row), len(header))
	}

	var (
		timestamp                                                  time.Time
		accountType, deviceType, difficulty, levelName, checkpoint string
		checkpointDuration                                         time.Duration
		deaths, fails                                              uint
	)

	for i, column := range header {
		cell := row[i]
		parser, ok := fieldParsers[column]
		if !ok {
			return nil, fmt.Errorf("unexpected column name: %s", column)
		}

		val, err := parser(cell)
		if err != nil {
			return nil, fmt.Errorf("error parsing column %q: %v", column, err)
		}

		switch column {
		case "UTC Timestamp":
			timestamp = val.(time.Time)
		case "Account Type":
			accountType = val.(string)
		case "Device Type":
			deviceType = val.(string)
		case "Difficulty":
			difficulty = val.(string)
		case "Level Name":
			levelName = val.(string)
		case "Checkpoint":
			checkpoint = val.(string)
		case "Checkpoint Duration":
			checkpointDuration = val.(time.Duration)
		case "Deaths":
			deaths = uint(val.(int64))
		case "Fails":
			fails = uint(val.(int64))
		}
	}

	return &Checkpoint{
		Timestamp:          timestamp.UTC(),
		AccountType:        accountType,
		DeviceType:         deviceType,
		Difficulty:         difficulty,
		LevelName:          levelName,
		Checkpoint:         checkpoint,
		CheckpointDuration: checkpointDuration,
		Deaths:             deaths,
		Fails:              fails,
	}, nil

}

func FromHtml(doc *goquery.Document) (Checkpoints, error) {
	header, rows, err := helpers.FindTableAfterHeader(doc, "Call of Duty: Black Ops 6")
	if err != nil {
		return nil, err
	}

	if len(header) == 0 {
		return nil, errors.New("header row not found")
	}
	if len(rows) == 0 {
		return nil, errors.New("no rows found")
	}

	var result []*Checkpoint

	for i, row := range rows {
		res, err := fromRow(header, row)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
		result = append(result, res)
	}

	return result, nil
}

// writes the checkpoints to CSV at the provided path
func ToCSV(outputDir string, checkpoints Checkpoints) error {
	filename := path.Join(outputDir, "black_ops_6_campaign_checkpoints.csv")
	return helpers.ToCSV(filename, headerLabels, checkpoints)
}

// writes the checkpoints to parquet at the provided path
func ToParquet(outputDir string, checkpoints Checkpoints) error {
	filename := path.Join(outputDir, "black_ops_6_campaign_checkpoints.parquet")
	return helpers.ToParquet(filename, checkpoints, new(checkpointExport))
}
