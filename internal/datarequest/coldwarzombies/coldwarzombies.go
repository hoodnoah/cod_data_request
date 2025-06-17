package coldwarzombies

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

var headerLabels = []string{
	"timestamp_utc",
	"device_type",
	"deaths",
	"headshots",
	"kills",
	"operator",
	"rank_at_start",
	"rank_at_end",
	"score",
	"suicides",
	"xp_at_start",
	"xp_at_end",
	"weapon",
	"field_upgrade",
	"round_number",
	"game_type",
	"map",
}

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp": helpers.TimeParser(),
	"Device Type":   helpers.StringParser(),
	"Deaths":        helpers.IntParser(),
	"Headshots":     helpers.IntParser(),
	"Kills":         helpers.IntParser(),
	"Operator":      helpers.StringParser(),
	"Rank At Start": helpers.IntParser(),
	"Rank At End":   helpers.IntParser(),
	"Score":         helpers.IntParser(),
	"Suicides":      helpers.IntParser(),
	"XP At Start":   helpers.IntParser(),
	"XP At End":     helpers.IntParser(),
	"Weapon":        helpers.StringParser(),
	"Field Upgrade": helpers.StringParser(),
	"Round Number":  helpers.IntParser(),
	"Game Type":     helpers.StringParser(),
	"Map":           helpers.StringParser(),
}

type ColdWarZombiesEvent struct {
	Timestamp    time.Time `col:"UTC Timestamp"`
	DeviceType   string    `col:"Device Type"`
	Deaths       int       `col:"Deaths"`
	Headshots    int       `col:"Headshots"`
	Kills        int       `col:"Kills"`
	Operator     string    `col:"Operator"`
	RankAtStart  int       `col:"Rank At Start"`
	RankAtEnd    int       `col:"Rank At End"`
	Score        int       `col:"Score"`
	Suicides     int       `col:"Suicides"`
	XPAtStart    int       `col:"XP At Start"`
	XPAtEnd      int       `col:"XP At End"`
	Weapon       string    `col:"Weapon"`
	FieldUpgrade string    `col:"Field Upgrade"`
	RoundNumber  int       `col:"Round Number"`
	GameType     string    `col:"Game Type"`
	Map          string    `col:"Map"`
}

type ColdWarZombiesEvents []*ColdWarZombiesEvent

type ColdWarZombiesEventExport struct {
	Timestamp    int64  `parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	DeviceType   string `parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Deaths       int64  `parquet:"name=deaths, type=INT64"`
	Headshots    int64  `parquet:"name=headshots, type=INT64"`
	Kills        int64  `parquet:"name=kills, type=INT64"`
	Operator     string `parquet:"name=operator, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RankAtStart  int64  `parquet:"name=rank_at_start, type=INT64"`
	RankAtEnd    int64  `parquet:"name=rank_at_end, type=INT64"`
	Score        int64  `parquet:"name=score, type=INT64"`
	Suicides     int64  `parquet:"name=suicides, type=INT64"`
	XPAtStart    int64  `parquet:"name=rank_at_start, type=INT64"`
	XPAtEnd      int64  `parquet:"name=rank_at_end, type=INT64"`
	Weapon       string `parquet:"name=weapon, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	FieldUpgrade string `parquet:"name=field_upgrade, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RoundNumber  int64  `parquet:"name=round_number, type=INT64"`
	GameType     string `parquet:"name=game_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Map          string `parquet:"name=map, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (c *ColdWarZombiesEvent) ToExport() any {
	return &ColdWarZombiesEventExport{
		Timestamp:    c.Timestamp.UnixMilli(),
		DeviceType:   c.DeviceType,
		Deaths:       int64(c.Deaths),
		Headshots:    int64(c.Headshots),
		Kills:        int64(c.Kills),
		Operator:     c.Operator,
		RankAtStart:  int64(c.RankAtStart),
		RankAtEnd:    int64(c.RankAtEnd),
		Score:        int64(c.Score),
		Suicides:     int64(c.Suicides),
		XPAtStart:    int64(c.XPAtStart),
		XPAtEnd:      int64(c.XPAtEnd),
		Weapon:       c.Weapon,
		FieldUpgrade: c.FieldUpgrade,
		RoundNumber:  int64(c.RoundNumber),
		GameType:     c.GameType,
		Map:          c.Map,
	}
}

func (c *ColdWarZombiesEvent) ToStringSlice() []string {
	return []string{
		c.Timestamp.Format("2006-01-02 15:04:05"),
		c.DeviceType,
		strconv.FormatUint(uint64(c.Deaths), 10),
		strconv.FormatUint(uint64(c.Headshots), 10),
		strconv.FormatUint(uint64(c.Kills), 10),
		c.Operator,
		strconv.FormatUint(uint64(c.RankAtStart), 10),
		strconv.FormatUint(uint64(c.RankAtEnd), 10),
		strconv.FormatUint(uint64(c.Score), 10),
		strconv.FormatUint(uint64(c.Suicides), 10),
		strconv.FormatUint(uint64(c.XPAtStart), 10),
		strconv.FormatUint(uint64(c.XPAtEnd), 10),
		c.Weapon,
		c.FieldUpgrade,
		strconv.FormatUint(uint64(c.RoundNumber), 10),
		c.GameType,
		c.Map,
	}
}

func fromRow(header []string, row []string) (*ColdWarZombiesEvent, error) {
	if len(row) == 0 {
		return nil, errors.New("no rows to parse")
	}

	if len(row) != len(header) {
		return nil, fmt.Errorf("row/header length mismatch: %d (header) vs %d (row)", len(header), len(row))
	}

	return helpers.ParseRowReflect[ColdWarZombiesEvent](header, row, "col", fieldParsers)
}

func FromHtml(doc *goquery.Document) (ColdWarZombiesEvents, error) {
	header, rows, err := helpers.FindTable(doc, "Call of Duty: Black Ops Cold War", "Zombies Data (reverse chronological)")
	if err != nil {
		return nil, err
	}

	if len(header) == 0 {
		return nil, errors.New("header row not found")
	}
	if len(rows) == 0 {
		return nil, errors.New("no rows found")
	}

	var result ColdWarZombiesEvents
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

func ToCSV(outputDir string, events *ColdWarZombiesEvents) error {
	filename := path.Join(outputDir, "cold_war_zombies_events.csv")
	return helpers.ToCSV(filename, headerLabels, *events)
}

func ToParquet(outputDir string, events *ColdWarZombiesEvents) error {
	filename := path.Join(outputDir, "cold_war_zombies_events.parquet")
	return helpers.ToParquet(filename, *events, new(ColdWarZombiesEventExport))
}
