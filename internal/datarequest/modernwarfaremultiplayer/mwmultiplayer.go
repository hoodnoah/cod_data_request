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
	"UTC Timestamp":         helpers.TimestampToUnixMillisInt64(),
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
	Timestamp          int64  `col:"UTC Timestamp" parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MatchID            string `col:"Match ID" parquet:"name=match_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Platform           string `col:"Platform" parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GameTypeScreenName string `col:"Game Type Screen Name" parquet:"name=game_type_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MapScreenName      string `col:"Map Screen Name" parquet:"name=map_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Rank               int64  `col:"Rank" parquet:"name=rank, type=INT64"`
	Score              int64  `col:"Score" parquet:"name=score, type=INT64"`
	Assists            int64  `col:"Assists" parquet:"name=assists, type=INT64"`
	Kills              int64  `col:"Kills" parquet:"name=kills, type=INT64"`
	Deaths             int64  `col:"Deaths" parquet:"name=deaths, type=INT64"`
	Headshots          int64  `col:"Headshots" parquet:"name=headshots, type=INT64"`
	LongestStreak      int64  `col:"Longest Streak" parquet:"name=longest_streak, type=INT64"`
	TotalXPEarned      int64  `col:"Total XP Earned" parquet:"name=total_xp_earned, type=INT64"`
}

type MWMultiplayerMatches []*MWMultiplayerMatch

func (m *MWMultiplayerMatch) ToStringSlice() []string {
	return []string{
		time.UnixMilli(m.Timestamp).UTC().Format(time.RFC3339),
		m.MatchID,
		m.Platform,
		m.GameTypeScreenName,
		m.MapScreenName,
		strconv.FormatInt(m.Rank, 10),
		strconv.FormatInt(m.Score, 10),
		strconv.FormatInt(m.Assists, 10),
		strconv.FormatInt(m.Kills, 10),
		strconv.FormatInt(m.Deaths, 10),
		strconv.FormatInt(m.Headshots, 10),
		strconv.FormatInt(m.LongestStreak, 10),
		strconv.FormatInt(m.TotalXPEarned, 10),
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
	return helpers.ToParquet(filename, *matches, new(MWMultiplayerMatch))
}
