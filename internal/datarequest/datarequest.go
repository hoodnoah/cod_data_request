package datarequest

import (
	// external
	"github.com/PuerkitoBio/goquery"

	// internal
	blops "github.com/hoodnoah/cod_data_request/internal/datarequest/blops6campaign"
	blopsMP "github.com/hoodnoah/cod_data_request/internal/datarequest/blops6multiplayer"
	cwZombies "github.com/hoodnoah/cod_data_request/internal/datarequest/coldwarzombies"
	mwCampaign "github.com/hoodnoah/cod_data_request/internal/datarequest/modernwarfarecampaign"
	mwCoop "github.com/hoodnoah/cod_data_request/internal/datarequest/modernwarfarecoop"
	mwMp "github.com/hoodnoah/cod_data_request/internal/datarequest/modernwarfaremultiplayer"
)

type CodDataRequest struct {
	BlackOps6CampaignCheckpoints  blops.Checkpoints
	BlackOps6MultiplayerMatches   blopsMP.MultiplayerMatches
	ColdWarZombiesEvents          cwZombies.ColdWarZombiesEvents
	ModernWarfareCampaignSegments mwCampaign.ModernWarfareCampaignSegments
	ModernWarfareCoops            mwCoop.ModernWarfareCoops
	ModernWarfareMPMatches        mwMp.MWMultiplayerMatches
}

func NewCodDataRequest() CodDataRequest {
	return CodDataRequest{
		BlackOps6CampaignCheckpoints:  nil,
		BlackOps6MultiplayerMatches:   nil,
		ColdWarZombiesEvents:          nil,
		ModernWarfareCampaignSegments: nil,
		ModernWarfareCoops:            nil,
		ModernWarfareMPMatches:        nil,
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

	cwZombiesEvents, err := cwZombies.FromHtml(doc)
	if err != nil {
		return err
	}
	c.ColdWarZombiesEvents = cwZombiesEvents

	mwCampaignEvents, err := mwCampaign.FromHtml(doc)
	if err != nil {
		return err
	}
	c.ModernWarfareCampaignSegments = mwCampaignEvents

	mwCoop, err := mwCoop.FromHtml(doc)
	if err != nil {
		return err
	}
	c.ModernWarfareCoops = mwCoop

	mwMpmatches, err := mwMp.FromHtml(doc)
	if err != nil {
		return err
	}
	c.ModernWarfareMPMatches = mwMpmatches

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

	if err := cwZombies.ToCSV(outputDir, &c.ColdWarZombiesEvents); err != nil {
		return err
	}

	if err := mwCampaign.ToCSV(outputDir, &c.ModernWarfareCampaignSegments); err != nil {
		return err
	}

	if err := mwCoop.ToCSV(outputDir, &c.ModernWarfareCoops); err != nil {
		return err
	}

	if err := mwMp.ToCSV(outputDir, &c.ModernWarfareMPMatches); err != nil {
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

	if err := cwZombies.ToParquet(outputDir, &c.ColdWarZombiesEvents); err != nil {
		return err
	}

	if err := mwCampaign.ToParquet(outputDir, &c.ModernWarfareCampaignSegments); err != nil {
		return err
	}

	if err := mwCoop.ToParquet(outputDir, &c.ModernWarfareCoops); err != nil {
		return err
	}

	if err := mwMp.ToParquet(outputDir, &c.ModernWarfareMPMatches); err != nil {
		return err
	}

	return nil
}
