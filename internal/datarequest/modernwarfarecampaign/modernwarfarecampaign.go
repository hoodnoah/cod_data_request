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
	"UTC Timestamp":                     helpers.TimeParser(),
	"Platform":                          helpers.StringParser(),
	"Campaign Screen Name":              helpers.StringParser(),
	"Campaign Difficulty":               helpers.StringParser(),
	"Time to Complete Campaign Segment": helpers.FloatParser(),
	"Deaths During Campaign Segment":    helpers.IntParser(),
	"Fails During Campaign Segment":     helpers.IntParser(),
}

type ModernWarfareCampaignSegment struct {
	Timestamp                     time.Time `col:"UTC Timestamp"`
	Platform                      string    `col:"Platform"`
	CampaignScreenName            string    `col:"Campaign Screen Name"`
	CampaignDifficulty            string    `col:"Campaign Difficulty"`
	TimeToCompleteCampaignSegment float64   `col:"Time to Complete Campaign Segment"`
	DeathsDuringCampaignSegment   int       `col:"Deaths During Campaign Segment"`
	FailsDuringCampaignSegment    int       `col:"Fails During Campaign Segment"`
}

type ModernWarfareCampaignSegments []*ModernWarfareCampaignSegment

type ModernWarfareCampaignSegmentExport struct {
	Timestamp                     int64   `parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Platform                      string  `parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CampaignScreenName            string  `parquet:"name=campaign_screen_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CampaignDifficulty            string  `parquet:"name=campaign_difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TimeToCompleteCampaignSegment float64 `parquet:"name=time_to_complete_campaign_segment, type=FLOAT"`
	DeathsDuringCampaignSegment   int64   `parquet:"name=deaths_during_campaign_segment, type=INT64"`
	FailsDuringCampaignSegment    int64   `parquet:"name=fails_during_campaign_segment, type=INT64"`
}

func (m *ModernWarfareCampaignSegment) ToExport() any {
	return &ModernWarfareCampaignSegmentExport{
		Timestamp:                     m.Timestamp.UnixMilli(),
		Platform:                      m.Platform,
		CampaignScreenName:            m.CampaignScreenName,
		CampaignDifficulty:            m.CampaignDifficulty,
		TimeToCompleteCampaignSegment: float64(m.TimeToCompleteCampaignSegment),
		DeathsDuringCampaignSegment:   int64(m.DeathsDuringCampaignSegment),
		FailsDuringCampaignSegment:    int64(m.FailsDuringCampaignSegment),
	}
}

func (m *ModernWarfareCampaignSegment) ToStringSlice() []string {
	return []string{
		m.Timestamp.Format("2006-01-02 15:04:05"),
		m.Platform,
		m.CampaignScreenName,
		m.CampaignDifficulty,
		fmt.Sprintf("%1f", m.TimeToCompleteCampaignSegment),
		strconv.FormatUint(uint64(m.DeathsDuringCampaignSegment), 10),
		strconv.FormatUint(uint64(m.FailsDuringCampaignSegment), 10),
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
	return helpers.ToParquet(filename, *segments, new(ModernWarfareCampaignSegmentExport))
}
