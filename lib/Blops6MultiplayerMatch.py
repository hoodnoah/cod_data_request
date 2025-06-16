from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
import pandas as pd
from bs4 import BeautifulSoup
from lib.helpers import parse_float, parse_int, parse_utc

header_to_field = {
    "UTC Timestamp": "Timestamp",
    "Account Type": "AccountType",
    "Device Type": "DeviceType",
    "Game Type": "GameType",
    "Match ID": "MatchID",
    "Match Start Timestamp": "MatchStartTimestamp",
    "Match End Timestamp": "MatchEndTimestamp",
    "Map": "Map",
    "Team": "Team",
    "Match Outcome": "MatchOutcome",
    "Operator": "Operator",
    "Operator Skin": "OperatorSkin",
    "Execution": "Execution",
    "Skill": "Skill",
    "Score": "Score",
    "Shots": "Shots",
    "Hits": "Hits",
    "Assists": "Assists",
    "Longest Streak": "LongestStreak",
    "Kills": "Kills",
    "Deaths": "Deaths",
    "Headshots": "Headshots",
    "Executions": "Executions",
    "Suicides": "Suicides",
    "Damage Done": "DamageDone",
    "Damage Taken": "DamageTaken",
    "Armor Collected": "ArmorCollected",
    "Armor Equipped": "ArmorEquipped",
    "Armor Destroyed": "ArmorDestroyed",
    "Ground Vehicles Used": "GroundVehiclesUsed",
    "Air Vehicles Used": "AirVehiclesUsed",
    "Percentage Of Time Moving": "PercentageTimeMoving",
    "Total XP": "TotalXP",
    "Score XP": "ScoreXP",
    "Challenge XP": "ChallengeXP",
    "Match XP": "MatchXP",
    "Medal XP": "MedalXP",
    "Bonus XP": "BonusXP",
    "Misc XP": "MiscXP",
    "Accolade XP": "AccoladeXP",
    "Weapon XP": "WeaponXP",
    "Operator XP": "OperatorXP",
    "Clan XP": "ClanXP",
    "Battle Pass XP": "BattlePassXP",
    "Rank at Start": "RankAtStart",
    "Rank at End": "RankAtEnd",
    "XP at Start": "XPAtStart",
    "XP at End": "XPAtEnd",
    "Score at Start": "ScoreAtStart",
    "Score at End": "ScoreAtEnd",
    "Prestige at Start": "PrestigeAtStart",
    "Prestige at End": "PrestigeAtEnd",
    "Lifetime Wall Bangs": "LifetimeWallbangs",
    "Lifetime Games Played": "LifetimeGamesPlayed",
    "Lifetime Time Played": "LifetimeTimePlayed",
    "Lifetime Wins": "LifetimeWins",
    "Lifetime Losses": "LifetimeLosses",
    "Lifetime Kills": "LifetimeKills",
    "Lifetime Deaths": "LifetimeDeaths",
    "Lifetime Hits": "LifetimeHits",
    "Lifetime Misses": "LifetimeMisses",
    "Lifetime Near Misses": "LifetimeNearMisses",
}

field_map = {
    "UTC Timestamp": parse_utc,
    "Account Type": str,
    "Device Type": str,
    "Game Type": str,
    "Match ID": str,
    "Match Start Timestamp": parse_utc,
    "Match End Timestamp": parse_utc,
    "Map": str,
    "Team": str,
    "Match Outcome": str,
    "Operator": str,
    "Operator Skin": str,
    "Execution": str,
    "Skill": parse_int,
    "Score": parse_int,
    "Shots": parse_int,
    "Hits": parse_int,
    "Assists": parse_int,
    "Longest Streak": parse_int,
    "Kills": parse_int,
    "Deaths": parse_int,
    "Headshots": parse_int,
    "Executions": parse_int,
    "Suicides": parse_int,
    "Damage Done": parse_int,
    "Damage Taken": parse_int,
    "Armor Collected": parse_int,
    "Armor Equipped": parse_int,
    "Armor Destroyed": parse_int,
    "Ground Vehicles Used": parse_int,
    "Air Vehicles Used": parse_int,
    "Percentage Of Time Moving": parse_float,
    "Total XP": parse_int,
    "Score XP": parse_int,
    "Challenge XP": parse_int,
    "Match XP": parse_int,
    "Medal XP": parse_int,
    "Bonus XP": parse_int,
    "Misc XP": parse_int,
    "Accolade XP": parse_int,
    "Weapon XP": parse_int,
    "Operator XP": parse_int,
    "Clan XP": parse_int,
    "Battle Pass XP": parse_int,
    "Rank at Start": parse_int,
    "Rank at End": parse_int,
    "XP at Start": parse_int,
    "XP at End": parse_int,
    "Score at Start": parse_int,
    "Score at End": parse_int,
    "Prestige at Start": parse_int,
    "Prestige at End": parse_int,
    "Lifetime Wall Bangs": parse_int,
    "Lifetime Games Played": parse_int,
    "Lifetime Time Played": parse_int,
    "Lifetime Wins": parse_int,
    "Lifetime Losses": parse_int,
    "Lifetime Kills": parse_int,
    "Lifetime Deaths": parse_int,
    "Lifetime Hits": parse_int,
    "Lifetime Misses": parse_int,
    "Lifetime Near Misses": parse_int,
}


columns = list(field_map.keys())


@dataclass(frozen=True)
class BlackOps6MultiplayerMatch:
    Timestamp: datetime
    AccountType: str
    DeviceType: str
    GameType: str
    MatchID: str
    MatchStartTimestamp: datetime
    MatchEndTimestamp: datetime
    Map: str
    Team: str
    MatchOutcome: str
    Operator: str
    OperatorSkin: str
    Execution: str
    Skill: int
    Score: int
    Shots: int
    Hits: int
    Assists: int
    LongestStreak: int
    Kills: int
    Deaths: int
    Headshots: int
    Executions: int
    Suicides: int
    DamageDone: int
    DamageTaken: int
    ArmorCollected: int
    ArmorEquipped: int
    ArmorDestroyed: int
    GroundVehiclesUsed: int
    AirVehiclesUsed: int
    PercentageTimeMoving: float
    TotalXP: int
    ScoreXP: int
    ChallengeXP: int
    MatchXP: int
    MedalXP: int
    BonusXP: int
    MiscXP: int
    AccoladeXP: int
    WeaponXP: int
    OperatorXP: int
    ClanXP: int
    BattlePassXP: int
    RankAtStart: int
    RankAtEnd: int
    XPAtStart: int
    XPAtEnd: int
    ScoreAtStart: int
    ScoreAtEnd: int
    PrestigeAtStart: int
    PrestigeAtEnd: int
    LifetimeWallbangs: int
    LifetimeGamesPlayed: int
    LifetimeTimePlayed: int
    LifetimeWins: int
    LifetimeLosses: int
    LifetimeKills: int
    LifetimeDeaths: int
    LifetimeHits: int
    LifetimeMisses: int
    LifetimeNearMisses: int

    @staticmethod
    def from_html_row(tr) -> "BlackOps6MultiplayerMatch":
        values = [td.get_text(strip=True) for td in tr.find_all("td")]
        raw_data = dict(zip(columns, values))
        parsed_data = {key: field_map[key](raw_data[key]) for key in raw_data}

        return

    # Reading methods
    @staticmethod
    def from_html(htmlPath: Path) -> List["BlackOps6CampaignCheckpoint"] | None:
        with open(htmlPath, "r") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")
        heading = soup.find("h1", string=lambda s: "Call of Duty: Black Ops 6" in s)
        table = heading.find_next("table")

        # extract rows
        rows = []
        for tr in table.find_all("tr")[1:]:  # skip header row
            rows.append(tr)

        checkpoints: List["BlackOps6CampaignCheckpoint"] = []
        for row in rows:
            cp = BlackOps6CampaignCheckpoint.from_html_row(row)
            checkpoints.append(cp)

        return checkpoints

    # Writing methods
    @staticmethod
    def to_pandas_df(checkpoints: List["BlackOps6CampaignCheckpoint"]) -> pd.DataFrame:
        return pd.DataFrame([c.__dict__ for c in checkpoints])

    @staticmethod
    def to_csv(checkpoints: List["BlackOps6CampaignCheckpoint"], path: Path) -> None:
        df = BlackOps6CampaignCheckpoint.to_pandas_df(checkpoints)
        df.to_csv(path, index=False)

    @staticmethod
    def to_parquet(
        checkpoints: List["BlackOps6CampaignCheckpoint"], path: Path
    ) -> None:
        df = BlackOps6CampaignCheckpoint.to_pandas_df(checkpoints)
        df.to_parquet(path, index=False, engine="pyarrow")
