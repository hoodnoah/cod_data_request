package blops6multiplayer

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
)

const (
	h1Text = "Call of Duty: Black Ops 6"
	h2Text = "Multiplayer Match Data (reverse chronological)"
)

var headerLabels = []string{
	"timestamp_utc",
	"account_type",
	"device_type",
	"game_type",
	"match_ID",
	"match_start_timestamp",
	"match_End_Timestamp",
	"map",
	"team",
	"match_Outcome",
	"operator",
	"operator_skin",
	"execution",
	"skill",
	"score",
	"shots",
	"hits",
	"assists",
	"longest_streak",
	"kills",
	"deaths",
	"headshots",
	"executions",
	"suicides",
	"damage_done",
	"damage_taken",
	"armor_collected",
	"armor_equipped",
	"armor_destroyed",
	"ground_vehicles_used",
	"air_vehicles_used",
	"percentage_of_time_moving",
	"total_xp",
	"score_xp",
	"challenge_xp",
	"match_xp",
	"medal_xp",
	"bonus_xp",
	"misc_xp",
	"accolade_xp",
	"weapon_xp",
	"operator_xp",
	"clan_xp",
	"battle_Pass_xp",
	"rank_at_start",
	"rank_at_end",
	"xP_at_start",
	"xP_at_end",
	"score_at_start",
	"score_at_end",
	"prestige_at_start",
	"prestige_at_end",
	"lifetime_wall_bangs",
	"lifetime_games_played",
	"lifetime_time_played",
	"lifetime_wins",
	"lifetime_losses",
	"lifetime_kills",
	"lifetime_deaths",
	"lifetime_hits",
	"lifetime_misses",
	"lifetime_near_misses",
}

var fieldParsers = map[string]helpers.FieldParser{
	"UTC Timestamp":             helpers.TimestampToUnixMillisInt64(),
	"Account Type":              helpers.StringParser(),
	"Device Type":               helpers.StringParser(),
	"Game Type":                 helpers.StringParser(),
	"Match ID":                  helpers.StringParser(),
	"Match Start Timestamp":     helpers.TimestampToUnixMillisInt64(),
	"Match End Timestamp":       helpers.TimestampToUnixMillisInt64(),
	"Map":                       helpers.StringParser(),
	"Team":                      helpers.StringParser(),
	"Match Outcome":             helpers.StringParser(),
	"Operator":                  helpers.StringParser(),
	"Operator Skin":             helpers.StringParser(),
	"Execution":                 helpers.StringParser(),
	"Skill":                     helpers.IntParser(),
	"Score":                     helpers.IntParser(),
	"Shots":                     helpers.IntParser(),
	"Hits":                      helpers.IntParser(),
	"Assists":                   helpers.IntParser(),
	"Longest Streak":            helpers.IntParser(),
	"Kills":                     helpers.IntParser(),
	"Deaths":                    helpers.IntParser(),
	"Headshots":                 helpers.IntParser(),
	"Executions":                helpers.IntParser(),
	"Suicides":                  helpers.IntParser(),
	"Damage Done":               helpers.IntParser(),
	"Damage Taken":              helpers.IntParser(),
	"Armor Collected":           helpers.IntParser(),
	"Armor Equipped":            helpers.IntParser(),
	"Armor Destroyed":           helpers.IntParser(),
	"Ground Vehicles Used":      helpers.IntParser(),
	"Air Vehicles Used":         helpers.IntParser(),
	"Percentage Of Time Moving": helpers.FloatParser(),
	"Total XP":                  helpers.IntParser(),
	"Score XP":                  helpers.IntParser(),
	"Challenge XP":              helpers.IntParser(),
	"Match XP":                  helpers.IntParser(),
	"Medal XP":                  helpers.IntParser(),
	"Bonus XP":                  helpers.IntParser(),
	"Misc XP":                   helpers.IntParser(),
	"Accolade XP":               helpers.IntParser(),
	"Weapon XP":                 helpers.IntParser(),
	"Operator XP":               helpers.IntParser(),
	"Clan XP":                   helpers.IntParser(),
	"Battle Pass XP":            helpers.IntParser(),
	"Rank at Start":             helpers.IntParser(),
	"Rank at End":               helpers.IntParser(),
	"XP at Start":               helpers.IntParser(),
	"XP at End":                 helpers.IntParser(),
	"Score at Start":            helpers.IntParser(),
	"Score at End":              helpers.IntParser(),
	"Prestige at Start":         helpers.IntParser(),
	"Prestige at End":           helpers.IntParser(),
	"Lifetime Wall Bangs":       helpers.IntParser(),
	"Lifetime Games Played":     helpers.IntParser(),
	"Lifetime Time Played":      helpers.IntParser(),
	"Lifetime Wins":             helpers.IntParser(),
	"Lifetime Losses":           helpers.IntParser(),
	"Lifetime Kills":            helpers.IntParser(),
	"Lifetime Deaths":           helpers.IntParser(),
	"Lifetime Hits":             helpers.IntParser(),
	"Lifetime Misses":           helpers.IntParser(),
	"Lifetime Near Misses":      helpers.IntParser(),
}

type MultiplayerMatch struct {
	Timestamp              int64   `col:"UTC Timestamp" parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	AccountType            string  `col:"Account Type" parquet:"name=account_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType             string  `col:"Device Type" parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GameType               string  `col:"Game Type" parquet:"name=game_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchID                string  `col:"Match ID" parquet:"name=match_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchStart             int64   `col:"Match Start Timestamp" parquet:"name=match_start, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MatchEnd               int64   `col:"Match End Timestamp" parquet:"name=match_end, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Map                    string  `col:"Map" parquet:"name=map, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Team                   string  `col:"Team" parquet:"name=team, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchOutcome           string  `col:"Match Outcome" parquet:"name=match_outcome, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Operator               string  `col:"Operator" parquet:"name=operator, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OperatorSkin           string  `col:"Operator Skin" parquet:"name=operator_skin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Execution              string  `col:"Execution" parquet:"name=execution, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Skill                  int64   `col:"Skill" parquet:"name=skill, type=INT32"`
	Score                  int64   `col:"Score" parquet:"name=score, type=INT32"`
	Shots                  int64   `col:"Shots" parquet:"name=shots, type=INT32"`
	Hits                   int64   `col:"Hits" parquet:"name=hits, type=INT32"`
	Assists                int64   `col:"Assists" parquet:"name=assists, type=INT32"`
	LongestStreak          int64   `col:"Longest Streak" parquet:"name=longest_streak, type=INT32"`
	Kills                  int64   `col:"Kills" parquet:"name=kills, type=INT32"`
	Deaths                 int64   `col:"Deaths" parquet:"name=deaths, type=INT32"`
	Headshots              int64   `col:"Headshots" parquet:"name=headshots, type=INT32"`
	Executions             int64   `col:"Executions" parquet:"name=executions, type=INT32"`
	Suicides               int64   `col:"Suicides" parquet:"name=suicides, type=INT32"`
	DamageDone             int64   `col:"Damage Done" parquet:"name=damage_done, type=INT32"`
	DamageTaken            int64   `col:"Damage Taken" parquet:"name=damage_taken, type=INT32"`
	ArmorCollected         int64   `col:"Armor Collected" parquet:"name=armor_collected, type=INT32"`
	ArmorEquipped          int64   `col:"Armor Equipped" parquet:"name=armor_equipped, type=INT32"`
	ArmorDestroyed         int64   `col:"Armor Destroyed" parquet:"name=armor_destroyed, type=INT32"`
	GroundVehiclesUsed     int64   `col:"Ground Vehicles Used" parquet:"name=ground_vehicles_used, type=INT32"`
	AirVehiclesUsed        int64   `col:"Air Vehicles Used" parquet:"name=air_vehicles_used, type=INT32"`
	PercentageOfTimeMoving float64 `col:"Percentage Of Time Moving" parquet:"name=percentage_time_moving, type=FLOAT"`
	TotalXP                int64   `col:"Total XP" parquet:"name=total_xp, type=INT32"`
	ScoreXP                int64   `col:"Score XP" parquet:"name=score_xp, type=INT32"`
	ChallengeXP            int64   `col:"Challenge XP" parquet:"name=challenge_xp, type=INT32"`
	MatchXP                int64   `col:"Match XP" parquet:"name=match_xp, type=INT32"`
	MedalXP                int64   `col:"Medal XP" parquet:"name=medal_xp, type=INT32"`
	BonusXP                int64   `col:"Bonus XP" parquet:"name=bonus_xp, type=INT32"`
	MiscXP                 int64   `col:"Misc XP" parquet:"name=misc_xp, type=INT32"`
	AccoladeXP             int64   `col:"Accolade XP" parquet:"name=accolade_xp, type=INT32"`
	WeaponXP               int64   `col:"Weapon XP" parquet:"name=weapon_xp, type=INT32"`
	OperatorXP             int64   `col:"Operator XP" parquet:"name=operator_xp, type=INT32"`
	ClanXP                 int64   `col:"Clan XP" parquet:"name=clan_xp, type=INT32"`
	BattlePassXP           int64   `col:"Battle Pass XP" parquet:"name=battle_pass_xp, type=INT32"`
	RankAtStart            int64   `col:"Rank at Start" parquet:"name=rank_at_start, type=INT32"`
	RankAtEnd              int64   `col:"Rank at End" parquet:"name=rank_at_end, type=INT32"`
	XPAtStart              int64   `col:"XP at Start" parquet:"name=xp_at_start, type=INT32"`
	XPAtEnd                int64   `col:"XP at End" parquet:"name=xp_at_end, type=INT32"`
	ScoreAtStart           int64   `col:"Score at Start" parquet:"name=score_at_start, type=INT32"`
	ScoreAtEnd             int64   `col:"Score at End" parquet:"name=score_at_end, type=INT32"`
	PrestigeAtStart        int64   `col:"Prestige at Start" parquet:"name=prestige_at_start, type=INT32"`
	PrestigeAtEnd          int64   `col:"Prestige at End" parquet:"name=prestige_at_end, type=INT32"`
	LifetimeWallBangs      int64   `col:"Lifetime Wall Bangs" parquet:"name=lifetime_wallbangs, type=INT32"`
	LifetimeGamesPlayed    int64   `col:"Lifetime Games Played" parquet:"name=lifetime_games_played, type=INT32"`
	LifetimeTimePlayed     int64   `col:"Lifetime Time Played" parquet:"name=lifetime_time_player, type=INT32"`
	LifetimeWins           int64   `col:"Lifetime Wins" parquet:"name=lifetime_wins, type=INT32"`
	LifetimeLosses         int64   `col:"Lifetime Losses" parquet:"name=lifetime_losses, type=INT32"`
	LifetimeKills          int64   `col:"Lifetime Kills" parquet:"name=lifetime_kills, type=INT32"`
	LifetimeDeaths         int64   `col:"Lifetime Deaths" parquet:"name=lifetime_deaths, type=INT32"`
	LifetimeHits           int64   `col:"Lifetime Hits" parquet:"name=lifetime_hits, type=INT32"`
	LifetimeMisses         int64   `col:"Lifetime Misses" parquet:"name=lifetime_misses, type=INT32"`
	LifetimeNearMisses     int64   `col:"Lifetime Near Misses" parquet:"name=lifetime_near_misses, type=INT32"`
}

type MultiplayerMatches []*MultiplayerMatch

func (m *MultiplayerMatch) ToStringSlice() []string {
	ts := time.UnixMilli(m.Timestamp).UTC().Format(time.RFC3339)
	ms := time.UnixMilli(m.MatchStart).UTC().Format(time.RFC3339)
	me := time.UnixMilli(m.MatchEnd).UTC().Format(time.RFC3339)

	return []string{
		ts,
		m.AccountType,
		m.DeviceType,
		m.GameType,
		m.MatchID,
		ms,
		me,
		m.Map,
		m.Team,
		m.MatchOutcome,
		m.Operator,
		m.OperatorSkin,
		m.Execution,
		strconv.FormatInt(m.Skill, 10),
		strconv.FormatInt(m.Score, 10),
		strconv.FormatInt(m.Shots, 10),
		strconv.FormatInt(m.Hits, 10),
		strconv.FormatInt(m.Assists, 10),
		strconv.FormatInt(m.LongestStreak, 10),
		strconv.FormatInt(m.Kills, 10),
		strconv.FormatInt(m.Deaths, 10),
		strconv.FormatInt(m.Headshots, 10),
		strconv.FormatInt(m.Executions, 10),
		strconv.FormatInt(m.Suicides, 10),
		strconv.FormatInt(m.DamageDone, 10),
		strconv.FormatInt(m.DamageTaken, 10),
		strconv.FormatInt(m.ArmorCollected, 10),
		strconv.FormatInt(m.ArmorEquipped, 10),
		strconv.FormatInt(m.ArmorDestroyed, 10),
		strconv.FormatInt(m.GroundVehiclesUsed, 10),
		strconv.FormatInt(m.AirVehiclesUsed, 10),
		fmt.Sprintf("%.1f", m.PercentageOfTimeMoving),
		strconv.FormatInt(m.TotalXP, 10),
		strconv.FormatInt(m.ScoreXP, 10),
		strconv.FormatInt(m.ChallengeXP, 10),
		strconv.FormatInt(m.MatchXP, 10),
		strconv.FormatInt(m.MedalXP, 10),
		strconv.FormatInt(m.BonusXP, 10),
		strconv.FormatInt(m.MiscXP, 10),
		strconv.FormatInt(m.AccoladeXP, 10),
		strconv.FormatInt(m.WeaponXP, 10),
		strconv.FormatInt(m.OperatorXP, 10),
		strconv.FormatInt(m.ClanXP, 10),
		strconv.FormatInt(m.BattlePassXP, 10),
		strconv.FormatInt(m.RankAtStart, 10),
		strconv.FormatInt(m.RankAtEnd, 10),
		strconv.FormatInt(m.XPAtStart, 10),
		strconv.FormatInt(m.XPAtEnd, 10),
		strconv.FormatInt(m.ScoreAtStart, 10),
		strconv.FormatInt(m.ScoreAtEnd, 10),
		strconv.FormatInt(m.PrestigeAtStart, 10),
		strconv.FormatInt(m.PrestigeAtEnd, 10),
		strconv.FormatInt(m.LifetimeWallBangs, 10),
		strconv.FormatInt(m.LifetimeGamesPlayed, 10),
		strconv.FormatInt(m.LifetimeTimePlayed, 10),
		strconv.FormatInt(m.LifetimeWins, 10),
		strconv.FormatInt(m.LifetimeLosses, 10),
		strconv.FormatInt(m.LifetimeKills, 10),
		strconv.FormatInt(m.LifetimeDeaths, 10),
		strconv.FormatInt(m.LifetimeHits, 10),
		strconv.FormatInt(m.LifetimeMisses, 10),
		strconv.FormatInt(m.LifetimeNearMisses, 10),
	}
}

var fromRow = helpers.MakeFromRow[MultiplayerMatch]("col", fieldParsers)

func FromHtml(doc *goquery.Document) (MultiplayerMatches, error) {
	return helpers.FromHtmlTable(doc, h1Text, h2Text, fromRow)
}

func ToCSV(outputDir string, matches *MultiplayerMatches) error {
	filename := path.Join(outputDir, "black_ops_6_multiplayer_matches.csv")
	return helpers.ToCSV(filename, headerLabels, *matches)
}

func ToParquet(outputDir string, matches *MultiplayerMatches) error {
	filename := path.Join(outputDir, "black_ops_6_multiplayer_matches.parquet")
	return helpers.ToParquet(filename, *matches, new(MultiplayerMatch))
}
