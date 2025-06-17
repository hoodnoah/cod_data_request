package blops6multiplayer

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hoodnoah/cod_data_request/internal/helpers"
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
	"UTC Timestamp": func(s string) (any, error) {
		t, err := helpers.TryParseTimeUTC(s)
		if err != nil {
			return nil, err
		}
		return t, nil
	},
	"Account Type": func(s string) (any, error) { return s, nil },
	"Device Type":  func(s string) (any, error) { return s, nil },
	"Game Type":    func(s string) (any, error) { return s, nil },
	"Match ID":     func(s string) (any, error) { return s, nil },
	"Match Start Timestamp": func(s string) (any, error) {
		t, err := helpers.TryParseTimeUTC(s)
		if err != nil {
			return nil, err
		}
		return t, nil
	},
	"Match End Timestamp": func(s string) (any, error) {
		t, err := helpers.TryParseTimeUTC(s)
		if err != nil {
			return nil, err
		}
		return t, nil
	},
	"Map":           func(s string) (any, error) { return s, nil },
	"Team":          func(s string) (any, error) { return s, nil },
	"Match Outcome": func(s string) (any, error) { return s, nil },
	"Operator":      func(s string) (any, error) { return s, nil },
	"Operator Skin": func(s string) (any, error) { return s, nil },
	"Execution":     func(s string) (any, error) { return s, nil },
	"Skill": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Score": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Shots": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Hits": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Assists": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Longest Streak": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Kills": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Deaths": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Headshots": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Executions": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Suicides": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Damage Done": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Damage Taken": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Armor Collected": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Armor Equipped": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Armor Destroyed": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Ground Vehicles Used": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Air Vehicles Used": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Percentage Of Time Moving": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Total XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Score XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Challenge XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Match XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Medal XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Bonus XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Misc XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Accolade XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Weapon XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Operator XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Clan XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Battle Pass XP": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Rank at Start": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Rank at End": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"XP at Start": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"XP at End": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Score at Start": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Score at End": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Prestige at Start": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Prestige at End": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Wall Bangs": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Games Played": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Time Played": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Wins": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Losses": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Kills": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Deaths": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Hits": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Misses": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
	"Lifetime Near Misses": func(s string) (any, error) {
		i, err := helpers.TryParseInt(s)
		if err != nil {
			return nil, err
		}
		return i, nil
	},
}

type MultiplayerMatch struct {
	Timestamp              time.Time `col:"UTC Timestamp"`
	AccountType            string    `col:"Account Type"`
	DeviceType             string    `col:"Device Type"`
	GameType               string    `col:"Game Type"`
	MatchID                string    `col:"Match ID"`
	MatchStart             time.Time `col:"Match Start"`
	MatchEnd               time.Time `col:"Match End"`
	Map                    string    `col:"Map"`
	Team                   string    `col:"Team"`
	MatchOutcome           string    `col:"Match Outcome"`
	Operator               string    `col:"Operator"`
	OperatorSkin           string    `col:"Operator Skin"`
	Execution              string    `col:"Execution"`
	Skill                  int       `col:"Skill"`
	Score                  int       `col:"Score"`
	Shots                  int       `col:"Shots"`
	Hits                   int       `col:"Hits"`
	Assists                int       `col:"Assists"`
	LongestStreak          int       `col:"Longest Streak"`
	Kills                  int       `col:"Kills"`
	Deaths                 int       `col:"Deaths"`
	Headshots              int       `col:"Headshots"`
	Executions             int       `col:"Executions"`
	Suicides               int       `col:"Suicides"`
	DamageDone             int       `col:"Damage Done"`
	DamageTaken            int       `col:"Damage Taken"`
	ArmorCollected         int       `col:"Armor Collected"`
	ArmorEquipped          int       `col:"Armor Equipped"`
	ArmorDestroyed         int       `col:"Armor Destroyed"`
	GroundVehiclesUsed     int       `col:"Ground Vehicles Used"`
	AirVehiclesUsed        int       `col:"Air Vehicles Used"`
	PercentageOfTimeMoving float32   `col:"Percentage Of Time Moving"`
	TotalXP                int       `col:"Total XP"`
	ScoreXP                int       `col:"Score XP"`
	ChallengeXP            int       `col:"Challenge XP"`
	MatchXP                int       `col:"Match XP"`
	MedalXP                int       `col:"Medal XP"`
	BonusXP                int       `col:"Bonus XP"`
	MiscXP                 int       `col:"Misc XP"`
	AccoladeXP             int       `col:"Accolade XP"`
	WeaponXP               int       `col:"Weapon XP"`
	OperatorXP             int       `col:"Operator XP"`
	ClanXP                 int       `col:"Clan XP"`
	BattlePassXP           int       `col:"Battle Pass XP"`
	RankAtStart            int       `col:"Rank at Start"`
	RankAtEnd              int       `col:"Rank at End"`
	XPAtStart              int       `col:"XP at Start"`
	XPAtEnd                int       `col:"XP at End"`
	ScoreAtStart           int       `col:"Score at Start"`
	ScoreAtEnd             int       `col:"Score at End"`
	PrestigeAtStart        int       `col:"Prestige at Start"`
	PrestigeAtEnd          int       `col:"Prestige at End"`
	LifetimeWallBangs      int       `col:"Lifetime Wall Bangs"`
	LifetimeGamesPlayed    int       `col:"Lifetime Games Played"`
	LifetimeTimePlayed     int       `col:"Lifetime Time Played"`
	LifetimeWins           int       `col:"Lifetime Wins"`
	LifetimeLosses         int       `col:"Lifetime Losses"`
	LifetimeKills          int       `col:"Lifetime Kills"`
	LifetimeDeaths         int       `col:"Lifetime Deaths"`
	LifetimeHits           int       `col:"Lifetime Hits"`
	LifetimeMisses         int       `col:"Lifetime Misses"`
	LifetimeNearMisses     int       `col:"Lifetime Near Misses"`
}

type MultiplayerMatches []*MultiplayerMatch

type MultiplayerMatchExport struct {
	Timestamp              int64   `parquet:"name=timestamp_utc, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	AccountType            string  `parquet:"name=account_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType             string  `parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GameType               string  `parquet:"name=game_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchID                string  `parquet:"name=match_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchStart             int64   `parquet:"name=match_start, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MatchEnd               int64   `parquet:"name=match_end, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Map                    string  `parquet:"name=map, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Team                   string  `parquet:"name=team, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MatchOutcome           string  `parquet:"name=match_outcome, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Operator               string  `parquet:"name=operator, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OperatorSkin           string  `parquet:"name=operator_skin, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Execution              string  `parquet:"name=execution, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Skill                  int64   `parquet:"name=skill, type=INT32"`
	Score                  int64   `parquet:"name=score, type=INT32"`
	Shots                  int64   `parquet:"name=shots, type=INT32"`
	Hits                   int64   `parquet:"name=hits, type=INT32"`
	Assists                int64   `parquet:"name=assists, type=INT32"`
	LongestStreak          int64   `parquet:"name=longest_streak, type=INT32"`
	Kills                  int64   `parquet:"name=kills, type=INT32"`
	Deaths                 int64   `parquet:"name=deaths, type=INT32"`
	Headshots              int64   `parquet:"name=headshots, type=INT32"`
	Executions             int64   `parquet:"name=executions, type=INT32"`
	Suicides               int64   `parquet:"name=suicides, type=INT32"`
	DamageDone             int64   `parquet:"name=damage_done, type=INT32"`
	DamageTaken            int64   `parquet:"name=damage_taken, type=INT32"`
	ArmorCollected         int64   `parquet:"name=armor_collected, type=INT32"`
	ArmorEquipped          int64   `parquet:"name=armor_equipped, type=INT32"`
	ArmorDestroyed         int64   `parquet:"name=armor_destroyed, type=INT32"`
	GroundVehiclesUsed     int64   `parquet:"name=ground_vehicles_used, type=INT32"`
	AirVehiclesUsed        int64   `parquet:"name=air_vehicles_used, type=INT32"`
	PercentageOfTimeMoving float32 `parquet:"name=percentage_time_moving, type=FLOAT"`
	TotalXP                int64   `parquet:"name=total_xp, type=INT32"`
	ScoreXP                int64   `parquet:"name=score_xp, type=INT32"`
	ChallengeXP            int64   `parquet:"name=challenge_xp, type=INT32"`
	MatchXP                int64   `parquet:"name=match_xp, type=INT32"`
	MedalXP                int64   `parquet:"name=medal_xp, type=INT32"`
	BonusXP                int64   `parquet:"name=bonus_xp, type=INT32"`
	MiscXP                 int64   `parquet:"name=misc_xp, type=INT32"`
	AccoladeXP             int64   `parquet:"name=accolade_xp, type=INT32"`
	WeaponXP               int64   `parquet:"name=weapon_xp, type=INT32"`
	OperatorXP             int64   `parquet:"name=operator_xp, type=INT32"`
	ClanXP                 int64   `parquet:"name=clan_xp, type=INT32"`
	BattlePassXP           int64   `parquet:"name=battle_pass_xp, type=INT32"`
	RankAtStart            int64   `parquet:"name=rank_at_start, type=INT32"`
	RankAtEnd              int64   `parquet:"name=rank_at_end, type=INT32"`
	XPAtStart              int64   `parquet:"name=xp_at_start, type=INT32"`
	XPAtEnd                int64   `parquet:"name=xp_at_end, type=INT32"`
	ScoreAtStart           int64   `parquet:"name=score_at_start, type=INT32"`
	ScoreAtEnd             int64   `parquet:"name=score_at_end, type=INT32"`
	PrestigeAtStart        int64   `parquet:"name=prestige_at_start, type=INT32"`
	PrestigeAtEnd          int64   `parquet:"name=prestige_at_end, type=INT32"`
	LifetimeWallBangs      int64   `parquet:"name=lifetime_wallbangs, type=INT32"`
	LifetimeGamesPlayed    int64   `parquet:"name=lifetime_games_played, type=INT32"`
	LifetimeTimePlayed     int64   `parquet:"name=lifetime_time_player, type=INT32"`
	LifetimeWins           int64   `parquet:"name=lifetime_wins, type=INT32"`
	LifetimeLosses         int64   `parquet:"name=lifetime_losses, type=INT32"`
	LifetimeKills          int64   `parquet:"name=lifetime_kills, type=INT32"`
	LifetimeDeaths         int64   `parquet:"name=lifetime_deaths, type=INT32"`
	LifetimeHits           int64   `parquet:"name=lifetime_hits, type=INT32"`
	LifetimeMisses         int64   `parquet:"name=lifetime_misses, type=INT32"`
	LifetimeNearMisses     int64   `parquet:"name=lifetime_near_misses, type=INT32"`
}

func (m *MultiplayerMatch) ToExport() any {
	return &MultiplayerMatchExport{
		Timestamp:              m.Timestamp.UnixMilli(),
		AccountType:            m.AccountType,
		DeviceType:             m.DeviceType,
		GameType:               m.GameType,
		MatchID:                m.MatchID,
		MatchStart:             m.MatchStart.UnixMilli(),
		MatchEnd:               m.MatchEnd.UnixMilli(),
		Map:                    m.Map,
		Team:                   m.Team,
		MatchOutcome:           m.MatchOutcome,
		Operator:               m.Operator,
		OperatorSkin:           m.OperatorSkin,
		Execution:              m.Execution,
		Skill:                  int64(m.Skill),
		Score:                  int64(m.Score),
		Shots:                  int64(m.Shots),
		Hits:                   int64(m.Hits),
		Assists:                int64(m.Assists),
		LongestStreak:          int64(m.LongestStreak),
		Kills:                  int64(m.Kills),
		Deaths:                 int64(m.Deaths),
		Headshots:              int64(m.Headshots),
		Executions:             int64(m.Executions),
		Suicides:               int64(m.Suicides),
		DamageDone:             int64(m.DamageDone),
		DamageTaken:            int64(m.DamageTaken),
		ArmorCollected:         int64(m.ArmorCollected),
		ArmorEquipped:          int64(m.ArmorEquipped),
		ArmorDestroyed:         int64(m.ArmorDestroyed),
		GroundVehiclesUsed:     int64(m.GroundVehiclesUsed),
		AirVehiclesUsed:        int64(m.AirVehiclesUsed),
		PercentageOfTimeMoving: float32(m.PercentageOfTimeMoving),
		TotalXP:                int64(m.TotalXP),
		ScoreXP:                int64(m.ScoreXP),
		ChallengeXP:            int64(m.ChallengeXP),
		MatchXP:                int64(m.MatchXP),
		MedalXP:                int64(m.MedalXP),
		BonusXP:                int64(m.BonusXP),
		MiscXP:                 int64(m.MiscXP),
		AccoladeXP:             int64(m.AccoladeXP),
		WeaponXP:               int64(m.WeaponXP),
		OperatorXP:             int64(m.OperatorXP),
		ClanXP:                 int64(m.ClanXP),
		BattlePassXP:           int64(m.BattlePassXP),
		RankAtStart:            int64(m.RankAtStart),
		RankAtEnd:              int64(m.RankAtEnd),
		XPAtStart:              int64(m.XPAtStart),
		XPAtEnd:                int64(m.XPAtEnd),
		ScoreAtStart:           int64(m.ScoreAtStart),
		ScoreAtEnd:             int64(m.ScoreAtEnd),
		PrestigeAtStart:        int64(m.PrestigeAtStart),
		PrestigeAtEnd:          int64(m.PrestigeAtEnd),
		LifetimeWallBangs:      int64(m.LifetimeWallBangs),
		LifetimeGamesPlayed:    int64(m.LifetimeGamesPlayed),
		LifetimeTimePlayed:     int64(m.LifetimeTimePlayed),
		LifetimeWins:           int64(m.LifetimeWins),
		LifetimeLosses:         int64(m.LifetimeLosses),
		LifetimeKills:          int64(m.LifetimeKills),
		LifetimeDeaths:         int64(m.LifetimeDeaths),
		LifetimeHits:           int64(m.LifetimeHits),
		LifetimeMisses:         int64(m.LifetimeMisses),
		LifetimeNearMisses:     int64(m.LifetimeNearMisses),
	}
}

func (m *MultiplayerMatch) ToStringSlice() []string {
	return []string{
		m.Timestamp.Format("2006-01-02 15:04:05"),
		m.AccountType,
		m.DeviceType,
		m.GameType,
		m.MatchID,
		m.MatchStart.Format("2006-01-02 15:04:05"),
		m.MatchEnd.Format("2006-01-02 15:04:05"),
		m.Map,
		m.Team,
		m.MatchOutcome,
		m.Operator,
		m.OperatorSkin,
		m.Execution,
		strconv.FormatUint(uint64(m.Skill), 10),
		strconv.FormatUint(uint64(m.Score), 10),
		strconv.FormatUint(uint64(m.Shots), 10),
		strconv.FormatUint(uint64(m.Hits), 10),
		strconv.FormatUint(uint64(m.Assists), 10),
		strconv.FormatUint(uint64(m.LongestStreak), 10),
		strconv.FormatUint(uint64(m.Kills), 10),
		strconv.FormatUint(uint64(m.Deaths), 10),
		strconv.FormatUint(uint64(m.Headshots), 10),
		strconv.FormatUint(uint64(m.Executions), 10),
		strconv.FormatUint(uint64(m.Suicides), 10),
		strconv.FormatUint(uint64(m.DamageDone), 10),
		strconv.FormatUint(uint64(m.DamageTaken), 10),
		strconv.FormatUint(uint64(m.ArmorCollected), 10),
		strconv.FormatUint(uint64(m.ArmorEquipped), 10),
		strconv.FormatUint(uint64(m.ArmorDestroyed), 10),
		strconv.FormatUint(uint64(m.GroundVehiclesUsed), 10),
		strconv.FormatUint(uint64(m.AirVehiclesUsed), 10),
		fmt.Sprintf("%.1f", m.PercentageOfTimeMoving),
		strconv.FormatUint(uint64(m.TotalXP), 10),
		strconv.FormatUint(uint64(m.ScoreXP), 10),
		strconv.FormatUint(uint64(m.ChallengeXP), 10),
		strconv.FormatUint(uint64(m.MatchXP), 10),
		strconv.FormatUint(uint64(m.MedalXP), 10),
		strconv.FormatUint(uint64(m.BonusXP), 10),
		strconv.FormatUint(uint64(m.MiscXP), 10),
		strconv.FormatUint(uint64(m.AccoladeXP), 10),
		strconv.FormatUint(uint64(m.WeaponXP), 10),
		strconv.FormatUint(uint64(m.OperatorXP), 10),
		strconv.FormatUint(uint64(m.ClanXP), 10),
		strconv.FormatUint(uint64(m.BattlePassXP), 10),
		strconv.FormatUint(uint64(m.RankAtStart), 10),
		strconv.FormatUint(uint64(m.RankAtEnd), 10),
		strconv.FormatUint(uint64(m.XPAtStart), 10),
		strconv.FormatUint(uint64(m.XPAtEnd), 10),
		strconv.FormatUint(uint64(m.ScoreAtStart), 10),
		strconv.FormatUint(uint64(m.ScoreAtEnd), 10),
		strconv.FormatUint(uint64(m.PrestigeAtStart), 10),
		strconv.FormatUint(uint64(m.PrestigeAtEnd), 10),
		strconv.FormatUint(uint64(m.LifetimeWallBangs), 10),
		strconv.FormatUint(uint64(m.LifetimeGamesPlayed), 10),
		strconv.FormatUint(uint64(m.LifetimeTimePlayed), 10),
		strconv.FormatUint(uint64(m.LifetimeWins), 10),
		strconv.FormatUint(uint64(m.LifetimeLosses), 10),
		strconv.FormatUint(uint64(m.LifetimeKills), 10),
		strconv.FormatUint(uint64(m.LifetimeDeaths), 10),
		strconv.FormatUint(uint64(m.LifetimeHits), 10),
		strconv.FormatUint(uint64(m.LifetimeMisses), 10),
		strconv.FormatUint(uint64(m.LifetimeNearMisses), 10),
	}
}

// parses a Black Ops 6 Multiplayer Match from a header and its rows
func fromRow(header []string, row []string) (*MultiplayerMatch, error) {
	if len(row) == 0 {
		return nil, errors.New("no rows to parse")
	}

	if len(row) != len(header) {
		return nil, fmt.Errorf("row/header length mismatch: %d (header) vs %d (row)", len(header), len(row))
	}

	var (
		Timestamp              time.Time
		AccountType            string
		DeviceType             string
		GameType               string
		MatchID                string
		MatchStart             time.Time
		MatchEnd               time.Time
		Map                    string
		Team                   string
		MatchOutcome           string
		Operator               string
		OperatorSkin           string
		Execution              string
		Skill                  int
		Score                  int
		Shots                  int
		Hits                   int
		Assists                int
		LongestStreak          int
		Kills                  int
		Deaths                 int
		Headshots              int
		Executions             int
		Suicides               int
		DamageDone             int
		DamageTaken            int
		ArmorCollected         int
		ArmorEquipped          int
		ArmorDestroyed         int
		GroundVehiclesUsed     int
		AirVehiclesUsed        int
		PercentageOfTimeMoving float32
		TotalXP                int
		ScoreXP                int
		ChallengeXP            int
		MatchXP                int
		MedalXP                int
		BonusXP                int
		MiscXP                 int
		AccoladeXP             int
		WeaponXP               int
		OperatorXP             int
		ClanXP                 int
		BattlePassXP           int
		RankAtStart            int
		RankAtEnd              int
		XPAtStart              int
		XPAtEnd                int
		ScoreAtStart           int
		ScoreAtEnd             int
		PrestigeAtStart        int
		PrestigeAtEnd          int
		LifetimeWallBangs      int
		LifetimeGamesPlayed    int
		LifetimeTimePlayed     int
		LifetimeWins           int
		LifetimeLosses         int
		LifetimeKills          int
		LifetimeDeaths         int
		LifetimeHits           int
		LifetimeMisses         int
		LifetimeNearMisses     int
	)

	for i, column := range header {
		cell := row[i]
		parser, ok := fieldParsers[column]
		if !ok {
			return nil, fmt.Errorf("unexpected column name: %s", column)
		}

		val, err := parser(cell)
		if err != nil {
			return nil, fmt.Errorf("error parsing column %q on value %d: %v", column, i, err)
		}

		switch column {
		case "UTC Timestamp":
			Timestamp = val.(time.Time)
		case "Account Type":
			AccountType = val.(string)
		case "Device Type":
			DeviceType = val.(string)
		case "Game Type":
			GameType = val.(string)
		case "Match ID":
			MatchID = val.(string)
		case "Match Start Timestamp":
			MatchStart = val.(time.Time)
		case "Match End Timestamp":
			MatchEnd = val.(time.Time)
		case "Map":
			Map = val.(string)
		case "Team":
			Team = val.(string)
		case "Match Outcome":
			MatchOutcome = val.(string)
		case "Operator":
			Operator = val.(string)
		case "Operator Skin":
			OperatorSkin = val.(string)
		case "Execution":
			Execution = val.(string)
		case "Skill":
			Skill = val.(int)
		case "Score":
			Score = val.(int)
		case "Shots":
			Shots = val.(int)
		case "Hits":
			Hits = val.(int)
		case "Assists":
			Assists = val.(int)
		case "Longest Streak":
			LongestStreak = val.(int)
		case "Kills":
			Kills = val.(int)
		case "Deaths":
			Deaths = val.(int)
		case "Headshots":
			Headshots = val.(int)
		case "Executions":
			Executions = val.(int)
		case "Suicides":
			Suicides = val.(int)
		case "Damage Done":
			DamageDone = val.(int)
		case "Damage Taken":
			DamageTaken = val.(int)
		case "Armor Collected":
			ArmorCollected = val.(int)
		case "Armor Equipped":
			ArmorEquipped = val.(int)
		case "Armor Destroyed":
			ArmorDestroyed = val.(int)
		case "Ground Vehicles Used":
			GroundVehiclesUsed = val.(int)
		case "Air Vehicles Used":
			AirVehiclesUsed = val.(int)
		case "Percentage Of Time Moving":
			PercentageOfTimeMoving = val.(float32)
		case "Total XP":
			TotalXP = val.(int)
		case "Score XP":
			ScoreXP = val.(int)
		case "Challenge XP":
			ChallengeXP = val.(int)
		case "Match XP":
			MatchXP = val.(int)
		case "Medal XP":
			MedalXP = val.(int)
		case "Bonus XP":
			BonusXP = val.(int)
		case "Misc XP":
			MiscXP = val.(int)
		case "Accolade XP":
			AccoladeXP = val.(int)
		case "Weapon XP":
			WeaponXP = val.(int)
		case "Operator XP":
			OperatorXP = val.(int)
		case "Clan XP":
			ClanXP = val.(int)
		case "Battle Pass XP":
			BattlePassXP = val.(int)
		case "Rank at Start":
			RankAtStart = val.(int)
		case "Rank at End":
			RankAtEnd = val.(int)
		case "XP at Start":
			XPAtStart = val.(int)
		case "XP at End":
			XPAtEnd = val.(int)
		case "Score at Start":
			ScoreAtStart = val.(int)
		case "Score at End":
			ScoreAtEnd = val.(int)
		case "Prestige at Start":
			PrestigeAtStart = val.(int)
		case "Prestige at End":
			PrestigeAtEnd = val.(int)
		case "Lifetime Wall Bangs":
			LifetimeWallBangs = val.(int)
		case "Lifetime Games Played":
			LifetimeGamesPlayed = val.(int)
		case "Lifetime Time Played":
			LifetimeTimePlayed = val.(int)
		case "Lifetime Wins":
			LifetimeWins = val.(int)
		case "Lifetime Losses":
			LifetimeLosses = val.(int)
		case "Lifetime Kills":
			LifetimeKills = val.(int)
		case "Lifetime Deaths":
			LifetimeDeaths = val.(int)
		case "Lifetime Hits":
			LifetimeHits = val.(int)
		case "Lifetime Misses":
			LifetimeMisses = val.(int)
		case "Lifetime Near Misses":
			LifetimeNearMisses = val.(int)
		}
	}

	return &MultiplayerMatch{
		Timestamp:              Timestamp.UTC(),
		AccountType:            AccountType,
		DeviceType:             DeviceType,
		GameType:               GameType,
		MatchID:                MatchID,
		MatchStart:             MatchStart.UTC(),
		MatchEnd:               MatchEnd.UTC(),
		Map:                    Map,
		Team:                   Team,
		MatchOutcome:           MatchOutcome,
		Operator:               Operator,
		OperatorSkin:           OperatorSkin,
		Execution:              Execution,
		Skill:                  Skill,
		Score:                  Score,
		Shots:                  Shots,
		Hits:                   Hits,
		Assists:                Assists,
		LongestStreak:          LongestStreak,
		Kills:                  Kills,
		Deaths:                 Deaths,
		Headshots:              Headshots,
		Executions:             Executions,
		Suicides:               Suicides,
		DamageDone:             DamageDone,
		DamageTaken:            DamageTaken,
		ArmorCollected:         ArmorCollected,
		ArmorEquipped:          ArmorEquipped,
		ArmorDestroyed:         ArmorDestroyed,
		GroundVehiclesUsed:     GroundVehiclesUsed,
		AirVehiclesUsed:        AirVehiclesUsed,
		PercentageOfTimeMoving: PercentageOfTimeMoving,
		TotalXP:                TotalXP,
		ScoreXP:                ScoreXP,
		ChallengeXP:            ChallengeXP,
		MatchXP:                MatchXP,
		MedalXP:                MedalXP,
		BonusXP:                BonusXP,
		MiscXP:                 MiscXP,
		AccoladeXP:             AccoladeXP,
		WeaponXP:               WeaponXP,
		OperatorXP:             OperatorXP,
		ClanXP:                 ClanXP,
		BattlePassXP:           BattlePassXP,
		RankAtStart:            RankAtStart,
		RankAtEnd:              RankAtEnd,
		XPAtStart:              XPAtStart,
		XPAtEnd:                XPAtEnd,
		ScoreAtStart:           ScoreAtStart,
		ScoreAtEnd:             ScoreAtEnd,
		PrestigeAtStart:        PrestigeAtStart,
		PrestigeAtEnd:          PrestigeAtEnd,
		LifetimeWallBangs:      LifetimeWallBangs,
		LifetimeGamesPlayed:    LifetimeGamesPlayed,
		LifetimeTimePlayed:     LifetimeTimePlayed,
		LifetimeWins:           LifetimeWins,
		LifetimeLosses:         LifetimeLosses,
		LifetimeKills:          LifetimeKills,
		LifetimeDeaths:         LifetimeDeaths,
		LifetimeHits:           LifetimeHits,
		LifetimeMisses:         LifetimeMisses,
		LifetimeNearMisses:     LifetimeNearMisses,
	}, nil

}

func FromHtml(doc *goquery.Document) (MultiplayerMatches, error) {
	header, rows, err := helpers.FindTable(doc, "Call of Duty: Black Ops 6", "Multiplayer Match Data (reverse chronological)")
	if err != nil {
		return nil, err
	}

	if len(header) == 0 {
		return nil, errors.New("header row not found")
	}
	if len(rows) == 0 {
		return nil, errors.New("no rows found")
	}

	var result MultiplayerMatches
	for i, row := range rows {
		res, err := fromRow(header, row)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
		result = append(result, res)
	}

	return result, nil
}
