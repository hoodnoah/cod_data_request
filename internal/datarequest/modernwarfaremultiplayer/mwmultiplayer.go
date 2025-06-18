package modernwarfaremultiplayer

import (
	"path"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

const (
	h1Text = "Call of Duty: Modern Warfare"
	h2Text = "Multiplayer Match Data (reverse chronological)"
)

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp":         helpers.TimeParser(),
	"Match ID":              helpers.StringParser(),
	"Platform":              helpers.StringParser(),
	"Game Type Screen Name": helpers.StringParser(),
	"Map Screen Name":       helpers.StringParser(),
	"Rank":                  helpers.IntParser(),
	"Score":                 helpers.IntParser(),
	"Assists":               helpers.IntParser(),
	"Kills":                 helpers.IntParser(),
	"Deaths":                helpers.IntParser(),
	"Headshots":             helpers.IntParser(),
	"Longest Streak":        helpers.IntParser(),
	"Total XP Earned":       helpers.IntParser(),
}

var headerLabels = []string{
	"UTC Timestamp",
	"Match ID",
	"Platform",
	"Game Type Screen Name",
	"Map Screen Name",
	"Rank",
	"Score",
	"Assists",
	"Kills",
	"Deaths",
	"Headshots",
	"Longest Streak",
	"Total XP Earned",
}

type MWMultiplayerMatch struct {
	Timestamp          time.Time `col:"UTC Timestamp"`
	MatchID            string    `col:"Match ID"`
	Platform           string    `col:"Platform"`
	GameTypeScreenName string    `col:"Game Type Screen Name"`
	MapScreenName      string    `col:"Map Screen Name"`
	Rank               int       `col:"Rank"`
	Score              int       `col:"Score"`
	Assists            int       `col:"Assists"`
	Kills              int       `col:"Kills"`
	Deaths             int       `col:"Deaths"`
	Headshots          int       `col:"Headshots"`
	LongestStreak      int       `col:"Longest Streak"`
	TotalXPEarned      int       `col:"Total XP Earned"`
}

type MWMultiplayerMatches []*MWMultiplayerMatch

type MWMultiplayerMatchExport struct {
	Timestamp          int64  `parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MatchID            string `parquet:"name=match_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Platform           string `parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GameTypeScreenName string `parquet:"name=game_type_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MapScreenName      string `parquet:"name=map_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Rank               int64  `parquet:"name=rank, type=INT64"`
	Score              int64  `parquet:"name=score, type=INT64"`
	Assists            int64  `parquet:"name=assists, type=INT64"`
	Kills              int64  `parquet:"name=kills, type=INT64"`
	Deaths             int64  `parquet:"name=deaths, type=INT64"`
	Headshots          int64  `parquet:"name=headshots, type=INT64"`
	LongestStreak      int64  `parquet:"name=longest_streak, type=INT64"`
	TotalXPEarned      int64  `parquet:"name=total_xp_earned, type=INT64"`
}

func (m *MWMultiplayerMatch) ToExport() any {
	return &MWMultiplayerMatchExport{
		Timestamp:          m.Timestamp.UnixMilli(),
		MatchID:            m.MatchID,
		Platform:           m.Platform,
		GameTypeScreenName: m.GameTypeScreenName,
		MapScreenName:      m.MapScreenName,
		Rank:               int64(m.Rank),
		Score:              int64(m.Score),
		Assists:            int64(m.Assists),
		Kills:              int64(m.Kills),
		Deaths:             int64(m.Deaths),
		Headshots:          int64(m.Headshots),
		LongestStreak:      int64(m.LongestStreak),
		TotalXPEarned:      int64(m.TotalXPEarned),
	}
}

func (m *MWMultiplayerMatch) ToStringSlice() []string {
	return []string{
		m.Timestamp.Format("2006-01-02 15:04:05"),
		m.MatchID,
		m.Platform,
		m.GameTypeScreenName,
		m.MapScreenName,
		strconv.FormatUint(uint64(m.Rank), 10),
		strconv.FormatUint(uint64(m.Score), 10),
		strconv.FormatUint(uint64(m.Assists), 10),
		strconv.FormatUint(uint64(m.Kills), 10),
		strconv.FormatUint(uint64(m.Deaths), 10),
		strconv.FormatUint(uint64(m.Headshots), 10),
		strconv.FormatUint(uint64(m.LongestStreak), 10),
		strconv.FormatUint(uint64(m.TotalXPEarned), 10),
	}
}

var fromRow = helpers.MakeFromRow[MWMultiplayerMatch]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (MWMultiplayerMatches, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
}

func ToCSV(outputDir string, matches *MWMultiplayerMatches) error {
	filename := path.Join(outputDir, "modern_warfare_multiplayer_matches.csv")
	return helpers.ToCSV(filename, headerLabels, *matches)
}

func ToParquet(outputdir string, matches *MWMultiplayerMatches) error {
	filename := path.Join(outputdir, "modern_warfare_multiplayer_matches.parquet")
	return helpers.ToParquet(filename, *matches, new(MWMultiplayerMatchExport))
}
