package warzone2

import (
	"path"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

const (
	h1Text = "Call of Duty: Warzone 2.0"
	h2Text = "Multiplayer Match Data (reverse chronological)"
)

var headerLabels = []string{
	"UTC Timestamp",
	"Device Type",
	"Account Type",
	"Map",
	"Match Outcome",
	"Skill",
	"Score",
	"Shots",
	"Hits",
	"Assists",
	"Longest Streak",
	"Kills",
	"Deaths",
	"Headshots",
	"Executions",
	"Suicides",
	"Damage Done",
	"Damage Taken",
	"Total XP",
	"Score XP",
	"Challenge XP",
	"Match XP",
	"Medal XP",
	"Bonus XP",
	"Misc XP",
	"Accolade XP",
	"Weapon XP",
	"Operator XP",
	"Clan XP",
	"Battle Pass XP",
	"Rank at Start",
	"Rank at End",
	"XP at Start",
	"XP at End",
	"Score at Start",
	"Score at End",
	"Prestige at Start",
	"Prestige at End",
	"Lifetime Wall Bangs",
	"Lifetime Games Played",
	"Lifetime Time Played",
	"Lifetime Wins",
	"Lifetime Losses",
	"Lifetime Kills",
	"Lifetime Deaths",
	"Lifetime Hits",
	"Lifetime Misses",
	"Lifetime Near Misses",
}

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp":         helpers.TimestampToUnixMillisInt64(),
	"Device Type":           helpers.StringParser(),
	"Account Type":          helpers.StringParser(),
	"Map":                   helpers.StringParser(),
	"Match Outcome":         helpers.StringParser(),
	"Skill":                 helpers.IntParser(),
	"Score":                 helpers.IntParser(),
	"Shots":                 helpers.IntParser(),
	"Hits":                  helpers.IntParser(),
	"Assists":               helpers.IntParser(),
	"Longest Streak":        helpers.IntParser(),
	"Kills":                 helpers.IntParser(),
	"Deaths":                helpers.IntParser(),
	"Headshots":             helpers.IntParser(),
	"Executions":            helpers.IntParser(),
	"Suicides":              helpers.IntParser(),
	"Damage Done":           helpers.IntParser(),
	"Damage Taken":          helpers.IntParser(),
	"Total XP":              helpers.IntParser(),
	"Score XP":              helpers.IntParser(),
	"Challenge XP":          helpers.IntParser(),
	"Match XP":              helpers.IntParser(),
	"Medal XP":              helpers.IntParser(),
	"Bonus XP":              helpers.IntParser(),
	"Misc XP":               helpers.IntParser(),
	"Accolade XP":           helpers.IntParser(),
	"Weapon XP":             helpers.IntParser(),
	"Operator XP":           helpers.IntParser(),
	"Clan XP":               helpers.IntParser(),
	"Battle Pass XP":        helpers.IntParser(),
	"Rank at Start":         helpers.IntParser(),
	"Rank at End":           helpers.IntParser(),
	"XP at Start":           helpers.IntParser(),
	"XP at End":             helpers.IntParser(),
	"Score at Start":        helpers.IntParser(),
	"Score at End":          helpers.IntParser(),
	"Prestige at Start":     helpers.IntParser(),
	"Prestige at End":       helpers.IntParser(),
	"Lifetime Wall Bangs":   helpers.IntParser(),
	"Lifetime Games Played": helpers.IntParser(),
	"Lifetime Time Played":  helpers.IntParser(),
	"Lifetime Wins":         helpers.IntParser(),
	"Lifetime Losses":       helpers.IntParser(),
	"Lifetime Kills":        helpers.IntParser(),
	"Lifetime Deaths":       helpers.IntParser(),
	"Lifetime Hits":         helpers.IntParser(),
	"Lifetime Misses":       helpers.IntParser(),
	"Lifetime Near Misses":  helpers.IntParser(),
}

type Warzone2Match struct {
	Timestamp           int64  `col:"UTC Timestamp" parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	DeviceType          string `col:"Device Type" parquet:"name=device_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AccountType         string `col:"Account Type" parquet:"name=account_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Map                 string `col:"Map" parquet:"name=map, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchOutcome        string `col:"Match Outcome" parquet:"name=match_outcome, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Skill               int64  `col:"Skill" parquet:"name=skill, type=INT64"`
	Score               int64  `col:"Score" parquet:"name=score, type=INT64"`
	Shots               int64  `col:"Shots" parquet:"name=shots, type=INT64"`
	Hits                int64  `col:"Hits" parquet:"name=hits, type=INT64"`
	Assists             int64  `col:"Assists" parquet:"name=assists, type=INT64"`
	LongestStreak       int64  `col:"Longest Streak" parquet:"name=longest_streak, type=INT64"`
	Kills               int64  `col:"Kills" parquet:"name=kills, type=INT64"`
	Deaths              int64  `col:"Deaths" parquet:"name=deaths, type=INT64"`
	Headshots           int64  `col:"Headshots" parquet:"name=headshots, type=INT64"`
	Executions          int64  `col:"Executions" parquet:"name=executions, type=INT64"`
	Suicides            int64  `col:"Suicides" parquet:"name=suicides, type=INT64"`
	DamageDone          int64  `col:"Damage Done" parquet:"name=damage_done, type=INT64"`
	DamageTaken         int64  `col:"Damage Taken" parquet:"name=damage_taken, type=INT64"`
	TotalXP             int64  `col:"Total XP" parquet:"name=total_xp, type=INT64"`
	ScoreXP             int64  `col:"Score XP" parquet:"name=score_xp, type=INT64"`
	ChallengeXP         int64  `col:"Challenge XP" parquet:"name=challenge_xp, type=INT64"`
	MatchXP             int64  `col:"Match XP" parquet:"name=match_xp, type=INT64"`
	MedalXP             int64  `col:"Medal XP" parquet:"name=medal_xp, type=INT64"`
	BonusXP             int64  `col:"Bonus XP" parquet:"name=bonus_xp, type=INT64"`
	MiscXP              int64  `col:"Misc XP" parquet:"name=misc_xp, type=INT64"`
	AccoladeXP          int64  `col:"Accolade XP" parquet:"name=accolade_xp, type=INT64"`
	WeaponXP            int64  `col:"Weapon XP" parquet:"name=weapon_xp, type=INT64"`
	OperatorXP          int64  `col:"Operator XP" parquet:"name=operator_xp, type=INT64"`
	ClanXP              int64  `col:"Clan XP" parquet:"name=clan_xp, type=INT64"`
	BattlePassXP        int64  `col:"Battle Pass XP" parquet:"name=battle_pass_xp, type=INT64"`
	RankAtStart         int64  `col:"Rank at Start" parquet:"name=rank_at_start, type=INT64"`
	RankAtEnd           int64  `col:"Rank at End" parquet:"name=rank_at_end, type=INT64"`
	XPAtStart           int64  `col:"XP at Start" parquet:"name=xp_at_start, type=INT64"`
	XPAtEnd             int64  `col:"XP at End" parquet:"name=xp_at_end, type=INT64"`
	ScoreAtStart        int64  `col:"Score at Start" parquet:"name=score_at_start, type=INT64"`
	ScoreAtEnd          int64  `col:"Score at End" parquet:"name=score_at_end, type=INT64"`
	PrestigeAtStart     int64  `col:"Prestige at Start" parquet:"name=prestige_at_start, type=INT64"`
	PrestigeAtEnd       int64  `col:"Prestige at End" parquet:"name=prestige_at_end, type=INT64"`
	LifetimeWallBangs   int64  `col:"Lifetime Wall Bangs" parquet:"name=lifetime_wall_bangs, type=INT64"`
	LifetimeGamesPlayed int64  `col:"Lifetime Games Played" parquet:"name=lifetime_games_played, type=INT64"`
	LifetimeTimePlayed  int64  `col:"Lifetime Time Played" parquet:"name=lifetime_time_played, type=INT64"`
	LifetimeWins        int64  `col:"Lifetime Wins" parquet:"name=lifetime_wins, type=INT64"`
	LifetimeLosses      int64  `col:"Lifetime Losses" parquet:"name=lifetime_losses, type=INT64"`
	LifetimeKills       int64  `col:"Lifetime Kills" parquet:"name=lifetime_kills, type=INT64"`
	LifetimeDeaths      int64  `col:"Lifetime Deaths" parquet:"name=lifetime_deaths, type=INT64"`
	LifetimeHits        int64  `col:"Lifetime Hits" parquet:"name=lifetime_hits, type=INT64"`
	LifetimeMisses      int64  `col:"Lifetime Misses" parquet:"name=lifetime_misses, type=INT64"`
	LifetimeNearMisses  int64  `col:"Lifetime Near Misses" parquet:"name=lifetime_near_misses, type=INT64"`
}

type Warzone2Matches = []*Warzone2Match

func (w *Warzone2Match) ToStringSlice() []string {
	return []string{
		time.UnixMilli(w.Timestamp).UTC().Format(time.RFC3339),
		w.DeviceType,
		w.AccountType,
		w.Map,
		w.MatchOutcome,
		strconv.FormatInt(w.Skill, 10),
		strconv.FormatInt(w.Score, 10),
		strconv.FormatInt(w.Shots, 10),
		strconv.FormatInt(w.Hits, 10),
		strconv.FormatInt(w.Assists, 10),
		strconv.FormatInt(w.LongestStreak, 10),
		strconv.FormatInt(w.Kills, 10),
		strconv.FormatInt(w.Deaths, 10),
		strconv.FormatInt(w.Headshots, 10),
		strconv.FormatInt(w.Executions, 10),
		strconv.FormatInt(w.Suicides, 10),
		strconv.FormatInt(w.DamageDone, 10),
		strconv.FormatInt(w.DamageTaken, 10),
		strconv.FormatInt(w.TotalXP, 10),
		strconv.FormatInt(w.ScoreXP, 10),
		strconv.FormatInt(w.ChallengeXP, 10),
		strconv.FormatInt(w.MatchXP, 10),
		strconv.FormatInt(w.MedalXP, 10),
		strconv.FormatInt(w.BonusXP, 10),
		strconv.FormatInt(w.MiscXP, 10),
		strconv.FormatInt(w.AccoladeXP, 10),
		strconv.FormatInt(w.WeaponXP, 10),
		strconv.FormatInt(w.OperatorXP, 10),
		strconv.FormatInt(w.ClanXP, 10),
		strconv.FormatInt(w.BattlePassXP, 10),
		strconv.FormatInt(w.RankAtStart, 10),
		strconv.FormatInt(w.RankAtEnd, 10),
		strconv.FormatInt(w.XPAtStart, 10),
		strconv.FormatInt(w.XPAtEnd, 10),
		strconv.FormatInt(w.ScoreAtStart, 10),
		strconv.FormatInt(w.ScoreAtEnd, 10),
		strconv.FormatInt(w.PrestigeAtStart, 10),
		strconv.FormatInt(w.PrestigeAtEnd, 10),
		strconv.FormatInt(w.LifetimeWallBangs, 10),
		strconv.FormatInt(w.LifetimeGamesPlayed, 10),
		strconv.FormatInt(w.LifetimeTimePlayed, 10),
		strconv.FormatInt(w.LifetimeWins, 10),
		strconv.FormatInt(w.LifetimeLosses, 10),
		strconv.FormatInt(w.LifetimeKills, 10),
		strconv.FormatInt(w.LifetimeDeaths, 10),
		strconv.FormatInt(w.LifetimeHits, 10),
		strconv.FormatInt(w.LifetimeMisses, 10),
		strconv.FormatInt(w.LifetimeNearMisses, 10),
	}
}

var fromRow = helpers.MakeFromRow[Warzone2Match]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (Warzone2Matches, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
}

func ToCSV(outputDir string, matches *Warzone2Matches) error {
	filename := path.Join(outputDir, "warzone_2_multiplayer_matches.csv")
	return helpers.ToCSV(filename, headerLabels, *matches)
}

func ToParquet(outputDir string, matches *Warzone2Matches) error {
	filename := path.Join(outputDir, "warzone_2_multiplayer_matches.parquet")
	return helpers.ToParquet(filename, *matches, new(Warzone2Match))
}
