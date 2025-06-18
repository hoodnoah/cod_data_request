package modernwarfarecampaign

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
	h2Text = "Campaign Checkpoint Data (reverse chronological)"
)

var headerLabels = []string{
	"timestamp_utc",
	"platform",
	"campaign_screen_name",
	"campaign_difficulty",
	"time_to_complete_campaign_segment_s",
	"deaths_during_campaign_segment",
	"fails_during_campaign_segment",
}

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp":                     helpers.TimestampToUnixMillisInt64(),
	"Platform":                          helpers.StringParser(),
	"Campaign Screen Name":              helpers.StringParser(),
	"Campaign Difficulty":               helpers.StringParser(),
	"Time to Complete Campaign Segment": helpers.FloatParser(),
	"Deaths During Campaign Segment":    helpers.IntParser(),
	"Fails During Campaign Segment":     helpers.IntParser(),
}

type ModernWarfareCampaignSegment struct {
	Timestamp                     int64   `col:"UTC Timestamp" parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Platform                      string  `col:"Platform" parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CampaignScreenName            string  `col:"Campaign Screen Name" parquet:"name=campaign_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CampaignDifficulty            string  `col:"Campaign Difficulty" parquet:"name=campaign_difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TimeToCompleteCampaignSegment float64 `col:"Time to Complete Campaign Segment" parquet:"name=time_to_complete_campaign_segment, type=FLOAT"`
	DeathsDuringCampaignSegment   int64   `col:"Deaths During Campaign Segment" parquet:"name=deaths_during_campaign_segment, type=INT64"`
	FailsDuringCampaignSegment    int64   `col:"Fails During Campaign Segment" parquet:"name=fails_during_campaign_segment, type=INT64"`
}

type ModernWarfareCampaignSegments []*ModernWarfareCampaignSegment

func (m *ModernWarfareCampaignSegment) ToStringSlice() []string {
	return []string{
		time.UnixMilli(m.Timestamp).UTC().Format(time.RFC3339),
		m.Platform,
		m.CampaignScreenName,
		m.CampaignDifficulty,
		fmt.Sprintf("%.1f", m.TimeToCompleteCampaignSegment),
		strconv.FormatInt(m.DeathsDuringCampaignSegment, 10),
		strconv.FormatInt(m.FailsDuringCampaignSegment, 10),
	}
}

var fromRow = helpers.MakeFromRow[ModernWarfareCampaignSegment]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (ModernWarfareCampaignSegments, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
}

func ToCSV(outputDir string, segments *ModernWarfareCampaignSegments) error {
	filename := path.Join(outputDir, "modern_warfare_campaign_segments.csv")
	return helpers.ToCSV(filename, headerLabels, *segments)
}

func ToParquet(outputdir string, segments *ModernWarfareCampaignSegments) error {
	filename := path.Join(outputdir, "modern_warfare_campaign_segments.parquet")
	return helpers.ToParquet(filename, *segments, new(ModernWarfareCampaignSegment))
}
