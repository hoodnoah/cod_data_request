package datarequest

import (
	// external
	"github.com/PuerkitoBio/goquery"

	// internal
	blops "github.com/hoodnoah/cod_data_request/internal/datarequest/blops6campaign"
	blopsMP "github.com/hoodnoah/cod_data_request/internal/datarequest/blops6multiplayer"
)

type CodDataRequest struct {
	BlackOps6CampaignCheckpoints blops.Checkpoints
	BlackOps6MultiplayerMatches  blopsMP.MultiplayerMatches
}

func NewCodDataRequest() CodDataRequest {
	return CodDataRequest{
		BlackOps6CampaignCheckpoints: nil,
		BlackOps6MultiplayerMatches:  nil,
	}
}

// Reads all data record types from a provided HTML file
func (c *CodDataRequest) ParseHtml(doc *goquery.Document) error {
	blops6campaignCheckpoints, err := blops.FromHtml(doc)
	if err != nil {
		return err
	}
	c.BlackOps6CampaignCheckpoints = blops6campaignCheckpoints

	blops6MPMatches, err := blopsMP.FromHtml(doc)
	if err != nil {
		return err
	}
	c.BlackOps6MultiplayerMatches = blops6MPMatches

	return nil
}

// saves constituent data records to CSV, failing on the first error
func (c *CodDataRequest) ToCSV(outputDir string) error {
	if err := blops.ToCSV(outputDir, c.BlackOps6CampaignCheckpoints); err != nil {
		return err
	}

	if err := blopsMP.ToCSV(outputDir, &c.BlackOps6MultiplayerMatches); err != nil {
		return err
	}

	return nil
}

// saves constituent data records to parquet, failing on the first error
func (c *CodDataRequest) ToParquet(outputDir string) error {
	if err := blops.ToParquet(outputDir, c.BlackOps6CampaignCheckpoints); err != nil {
		return err
	}

	if err := blopsMP.ToParquet(outputDir, &c.BlackOps6MultiplayerMatches); err != nil {
		return err
	}

	return nil
}
