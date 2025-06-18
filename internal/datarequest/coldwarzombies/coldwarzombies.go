package coldwarzombies

import (
	"path"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

const (
	h1Text = "Call of Duty: Black Ops Cold War"
	h2Text = "Zombies Data (reverse chronological)"
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
	"UTC Timestamp": helpers.TimestampToUnixMillisInt64(),
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
	Timestamp    int64  `col:"UTC Timestamp" parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	DeviceType   string `col:"Device Type" parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Deaths       int64  `col:"Deaths" parquet:"name=deaths, type=INT64"`
	Headshots    int64  `col:"Headshots" parquet:"name=headshots, type=INT64"`
	Kills        int64  `col:"Kills" parquet:"name=kills, type=INT64"`
	Operator     string `col:"Operator" parquet:"name=operator, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RankAtStart  int64  `col:"Rank At Start" parquet:"name=rank_at_start, type=INT64"`
	RankAtEnd    int64  `col:"Rank At End" parquet:"name=rank_at_end, type=INT64"`
	Score        int64  `col:"Score" parquet:"name=score, type=INT64"`
	Suicides     int64  `col:"Suicides" parquet:"name=suicides, type=INT64"`
	XPAtStart    int64  `col:"XP At Start" parquet:"name=xp_at_start, type=INT64"`
	XPAtEnd      int64  `col:"XP At End" parquet:"name=xp_at_end, type=INT64"`
	Weapon       string `col:"Weapon" parquet:"name=weapon, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	FieldUpgrade string `col:"Field Upgrade" parquet:"name=field_upgrade, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RoundNumber  int64  `col:"Round Number" parquet:"name=round_number, type=INT64"`
	GameType     string `col:"Game Type" parquet:"name=game_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Map          string `col:"Map" parquet:"name=map, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type ColdWarZombiesEvents []*ColdWarZombiesEvent

func (c *ColdWarZombiesEvent) ToStringSlice() []string {
	ts := time.UnixMilli(c.Timestamp).UTC().Format(time.RFC3339)

	return []string{
		ts,
		c.DeviceType,
		strconv.FormatInt(c.Deaths, 10),
		strconv.FormatInt(c.Headshots, 10),
		strconv.FormatInt(c.Kills, 10),
		c.Operator,
		strconv.FormatInt(c.RankAtStart, 10),
		strconv.FormatInt(c.RankAtEnd, 10),
		strconv.FormatInt(c.Score, 10),
		strconv.FormatInt(c.Suicides, 10),
		strconv.FormatInt(c.XPAtStart, 10),
		strconv.FormatInt(c.XPAtEnd, 10),
		c.Weapon,
		c.FieldUpgrade,
		strconv.FormatInt(c.RoundNumber, 10),
		c.GameType,
		c.Map,
	}
}

var fromRow = helpers.MakeFromRow[ColdWarZombiesEvent]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (ColdWarZombiesEvents, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
}

func ToCSV(outputDir string, events *ColdWarZombiesEvents) error {
	filename := path.Join(outputDir, "cold_war_zombies_events.csv")
	return helpers.ToCSV(filename, headerLabels, *events)
}

func ToParquet(outputDir string, events *ColdWarZombiesEvents) error {
	filename := path.Join(outputDir, "cold_war_zombies_events.parquet")
	return helpers.ToParquet(filename, *events, new(ColdWarZombiesEvent))
}
