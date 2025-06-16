package datarequest

import (
	// external
	"github.com/PuerkitoBio/goquery"

	// internal
	blops "github.com/hoodnoah/cod_data_request/internal/datarequest/blops6campaign"
)

type CodDataRequest struct {
	BlackOps6CampaignCheckpoints blops.Checkpoints
}

func NewCodDataRequest() CodDataRequest {
	return CodDataRequest{
		BlackOps6CampaignCheckpoints: nil,
	}
}

// Reads all data record types from a provided HTML file
func (c *CodDataRequest) ParseHtml(doc *goquery.Document) error {
	blops6campaignCheckpoints, err := blops.FromHtml(doc)
	if err != nil {
		return err
	}

	c.BlackOps6CampaignCheckpoints = blops6campaignCheckpoints

	return nil
}

// saves constituent data records to CSV, failing on the first error
func (c *CodDataRequest) ToCSV(outputDir string) error {
	err := blops.ToCSV(outputDir, c.BlackOps6CampaignCheckpoints)
	if err != nil {
		return err
	}

	return nil
}

// saves constituent data records to parquet, failing on the first error
func (c *CodDataRequest) ToParquet(outputDir string) error {
	err := blops.ToParquet(outputDir, c.BlackOps6CampaignCheckpoints)
	if err != nil {
		return err
	}

	return nil
}
