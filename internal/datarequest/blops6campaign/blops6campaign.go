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

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp":       helpers.TimeParser(),
	"Account Type":        helpers.StringParser(),
	"Device Type":         helpers.StringParser(),
	"Difficulty":          helpers.StringParser(),
	"Level Name":          helpers.StringParser(),
	"Checkpoint":          helpers.StringParser(),
	"Checkpoint Duration": helpers.FloatParser(),
	"Deaths":              helpers.IntParser(),
	"Fails":               helpers.IntParser(),
}

type Checkpoint struct {
	Timestamp          time.Time     `col:"UTC Timestamp"`
	AccountType        string        `col:"Account Type"`
	DeviceType         string        `col:"Device Type"`
	Difficulty         string        `col:"Difficulty"`
	LevelName          string        `col:"Level Name"`
	Checkpoint         string        `col:"Checkpoint"`
	CheckpointDuration time.Duration `col:"Checkpoint Duration"`
	Deaths             uint          `col:"Deaths"`
	Fails              uint          `col:"Fails"`
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

	return helpers.ParseRowReflect[Checkpoint](header, row, "col", fieldParsers)
}

func FromHtml(doc *goquery.Document) (Checkpoints, error) {
	header, rows, err := helpers.FindTable(doc, "Call of Duty: Black Ops 6", "Campaign Checkpoint Data (reverse chronological)")
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
