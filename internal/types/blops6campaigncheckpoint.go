package types

import (
	// std
	"encoding/csv"
	"errors"
	"fmt"
	"os"
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

type Blops6CampaignCheckpoint struct {
	Timestamp          time.Time     `parquet:"name=timestamp_utc, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	AccountType        string        `parquet:"name=account_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType         string        `parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Difficulty         string        `parquet:"name=difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LevelName          string        `parquet:"name=level_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Checkpoint         string        `parquet:"name=checkpoint, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CheckpointDuration time.Duration `parquet:"name=checkpoint_duration, type=FLOAT"`
	Deaths             uint          `parquet:"name=deaths, type=UINT32"`
	Fails              uint          `parquet:"name=fails, type=UINT32"`
}

func (b *Blops6CampaignCheckpoint) ToStringSlice() []string {
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
func fromRow(header []string, row []string) (*Blops6CampaignCheckpoint, error) {
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

	return &Blops6CampaignCheckpoint{
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

func FromHtml(doc *goquery.Document) ([]*Blops6CampaignCheckpoint, error) {
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

	var result []*Blops6CampaignCheckpoint

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

func ToCSV(outputPath string, checkpoints []*Blops6CampaignCheckpoint) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header
	if err := writer.Write(headerLabels); err != nil {
		return err
	}

	// Write the data rows
	for _, c := range checkpoints {
		if err := writer.Write(c.ToStringSlice()); err != nil {
			return err
		}
	}
	return nil
}
