package modernwarfarecoop

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

const (
	h1Text = "Call of Duty: Modern Warfare"
	h2Text = "CoOp Match Data (reverse chronological)"
)

var headerLabels = []string{
	"UTC Timestamp",
	"Platform",
	"CoOp Level Screen Name",
	"Gametype Screen Name",
	"Active Objective",
	"Role Field Upgrade Used",
	"Munition Used",
	"Rank",
	"Total XP",
	"Total Kills",
	"Total Revives",
	"Total Last Stands",
	"Average Speed During Match",
}

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp":              helpers.TimestampToUnixMillisInt64(),
	"Platform":                   helpers.StringParser(),
	"CoOp Level Screen Name":     helpers.StringParser(),
	"Gametype Screen Name":       helpers.StringParser(),
	"Active Objective":           helpers.StringParser(),
	"Role Field Upgrade Used":    helpers.StringParser(),
	"Munition Used":              helpers.StringParser(),
	"Rank":                       helpers.IntParser(),
	"Total XP":                   helpers.IntParser(),
	"Total Kills":                helpers.IntParser(),
	"Total Revives":              helpers.IntParser(),
	"Total Last Stands":          helpers.IntParser(),
	"Average Speed During Match": helpers.FloatParser(),
}

type ModernWafareCoop struct {
	Timestamp               int64   `col:"UTC Timestamp" parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Platform                string  `col:"Platform" parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CoopLevelScreenName     string  `col:"CoOp Level Screen Name" parquet:"name=coop_level_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GametypeScreenName      string  `col:"Gametype Screen Name" parquet:"name=gametype_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ActiveObjective         string  `col:"Active Objective" parquet:"name=active_objective, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RoleFieldUpgradeUsed    string  `col:"Role Field Upgrade Used" parquet:"name=role_field_upgrade_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MunitionUsed            string  `col:"Munition Used" parquet:"name=munition_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Rank                    int64   `col:"Rank" parquet:"name=rank, type=INT64"`
	TotalXP                 int64   `col:"Total XP" parquet:"name=total_xp, type=INT64"`
	TotalKills              int64   `col:"Total Kills" parquet:"name=total_kills, type=INT64"`
	TotalRevives            int64   `col:"Total Revives" parquet:"name=total_revives, type=INT64"`
	TotalLastStands         int64   `col:"Total Last Stands" parquet:"name=total_last_stands, type=INT64"`
	AverageSpeedDuringMatch float64 `col:"Average Speed During Match" parquet:"name=average_speed_during_match, type=FLOAT"`
}

type ModernWarfareCoops []*ModernWafareCoop

func (m *ModernWafareCoop) ToStringSlice() []string {
	return []string{
		time.UnixMilli(m.Timestamp).UTC().Format(time.RFC3339),
		m.Platform,
		m.CoopLevelScreenName,
		m.GametypeScreenName,
		m.ActiveObjective,
		m.RoleFieldUpgradeUsed,
		m.MunitionUsed,
		strconv.FormatInt(m.Rank, 10),
		strconv.FormatInt(m.TotalXP, 10),
		strconv.FormatInt(m.TotalKills, 10),
		strconv.FormatInt(m.TotalRevives, 10),
		strconv.FormatInt(m.TotalLastStands, 10),
		fmt.Sprintf("%.5f", m.AverageSpeedDuringMatch),
	}
}

var fromRow = helpers.MakeFromRow[ModernWafareCoop]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (ModernWarfareCoops, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
}

func ToCSV(outputDir string, coops *ModernWarfareCoops) error {
	filename := path.Join(outputDir, "modern_warfare_coop.csv")
	return helpers.ToCSV(filename, headerLabels, *coops)
}

func ToParquet(outputDir string, coops *ModernWarfareCoops) error {
	filename := path.Join(outputDir, "modern_warfare_coop.parquet")
	return helpers.ToParquet(filename, *coops, new(ModernWafareCoop))
}
