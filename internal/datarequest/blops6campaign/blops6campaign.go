package blops6campaign

import (
	// std

	"fmt"
	"path"
	"strconv"
	"time"

	// external
	"github.com/PuerkitoBio/goquery"

	// internal
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

const (
	h1Text = "Call of Duty: Black Ops 6"
	h2Text = "Campaign Checkpoint Data (reverse chronological)"
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
	"UTC Timestamp":       helpers.TimestampToUnixMillisInt64(),
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
	Timestamp          int64   `col:"UTC Timestamp" parquet:"name=timestamp_ms_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	AccountType        string  `col:"Account Type" parquet:"name=account_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType         string  `col:"Device Type" parquet:"name=device_type , type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Difficulty         string  `col:"Difficulty" parquet:"name=difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LevelName          string  `col:"Level Name" parquet:"name=level_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Checkpoint         string  `col:"Checkpoint" parquet:"name=checkpoint, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CheckpointDuration float64 `col:"Checkpoint Duration" parquet:"name=checkpoint_duration, type=FLOAT"`
	Deaths             int64   `col:"Deaths" parquet:"name=deaths, type=INT64, convertedtype=UINT_64"`
	Fails              int64   `col:"Fails" parquet:"name=fails, type=INT64, convertedtype=UINT_64"`
}

type Checkpoints []*Checkpoint

type checkpointExport = Checkpoint

func (b *Checkpoint) ToExport() any {
	return b
}

func (b *Checkpoint) ToStringSlice() []string {
	t := time.UnixMilli(b.Timestamp).UTC().Format(time.RFC3339)

	return []string{
		t,
		b.AccountType,
		b.DeviceType,
		b.Difficulty,
		b.LevelName,
		b.Checkpoint,
		fmt.Sprintf("%.1f", b.CheckpointDuration),
		strconv.FormatInt(b.Deaths, 10),
		strconv.FormatInt(b.Fails, 10),
	}
}

var fromRow = helpers.MakeFromRow[Checkpoint]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (Checkpoints, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
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
