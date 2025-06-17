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
	"UTC Timestamp":              helpers.TimeParser(),
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
	Timestamp               time.Time `col:"UTC Timestamp"`
	Platform                string    `col:"Platform"`
	CoopLevelScreenName     string    `col:"CoOp Level Screen Name"`
	GametypeScreenName      string    `col:"Gametype Screen Name"`
	ActiveObjective         string    `col:"Active Objective"`
	RoleFieldUpgradeUsed    string    `col:"Role Field Upgrade Used"`
	MunitionUsed            string    `col:"Munition Used"`
	Rank                    int       `col:"Rank"`
	TotalXP                 int       `col:"Total XP"`
	TotalKills              int       `col:"Total Kills"`
	TotalRevives            int       `col:"Total Revives"`
	TotalLastStands         int       `col:"Total Last Stands"`
	AverageSpeedDuringMatch float64   `col:"Average Speed During Match"`
}

type ModernWarfareCoops []*ModernWafareCoop

type ModernWarfareCoopExport struct {
	Timestamp               int64   `parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Platform                string  `parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CoopLevelScreenName     string  `parquet:"name=coop_level_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GametypeScreenName      string  `parquet:"name=gametype_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ActiveObjective         string  `parquet:"name=active_objective,  type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RoleFieldUpgradeUsed    string  `parquet:"name=role_field_upgrade_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MunitionUsed            string  `parquet:"name=munition_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Rank                    int64   `parquet:"name=rank, type=INT64"`
	TotalXP                 int64   `parquet:"name=total_xp, type=INT64"`
	TotalKills              int64   `parquet:"name=total_kills, type=INT64"`
	TotalRevives            int64   `parquet:"name=total_revives, type=INT64"`
	TotalLastStands         int64   `parquet:"name=total_last_stands, type=INT64"`
	AverageSpeedDuringMatch float64 `parquet:"name=average_speed_during_match, type=FLOAT"`
}

func (m *ModernWafareCoop) ToExport() any {
	return &ModernWarfareCoopExport{
		Timestamp:               m.Timestamp.UnixMilli(),
		Platform:                m.Platform,
		CoopLevelScreenName:     m.CoopLevelScreenName,
		GametypeScreenName:      m.GametypeScreenName,
		ActiveObjective:         m.ActiveObjective,
		RoleFieldUpgradeUsed:    m.RoleFieldUpgradeUsed,
		MunitionUsed:            m.MunitionUsed,
		Rank:                    int64(m.Rank),
		TotalXP:                 int64(m.TotalXP),
		TotalKills:              int64(m.TotalKills),
		TotalRevives:            int64(m.TotalRevives),
		TotalLastStands:         int64(m.TotalLastStands),
		AverageSpeedDuringMatch: float64(m.AverageSpeedDuringMatch),
	}
}

func (m *ModernWafareCoop) ToStringSlice() []string {
	return []string{
		m.Timestamp.Format("2006-01-02 15:04:05"),
		m.Platform,
		m.CoopLevelScreenName,
		m.GametypeScreenName,
		m.ActiveObjective,
		m.RoleFieldUpgradeUsed,
		m.MunitionUsed,
		strconv.FormatUint(uint64(m.Rank), 10),
		strconv.FormatUint(uint64(m.TotalXP), 10),
		strconv.FormatUint(uint64(m.TotalKills), 10),
		strconv.FormatUint(uint64(m.TotalRevives), 10),
		strconv.FormatUint(uint64(m.TotalLastStands), 10),
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
	return helpers.ToParquet(filename, *coops, new(ModernWarfareCoopExport))
}
