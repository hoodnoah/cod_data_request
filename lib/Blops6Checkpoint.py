from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
import pandas as pd
from bs4 import BeautifulSoup
from lib.helpers import parse_float, parse_int, parse_utc


@dataclass(frozen=True)
class BlackOps6CampaignCheckpoint:
    Timestamp: datetime
    AccountType: str
    DeviceType: str
    Difficulty: str
    LevelName: str
    Checkpoint: str
    CheckpointDuration: float
    Deaths: int
    Fails: int

    @staticmethod
    def csv_headers() -> str:
        return ",".join(
            [
                "timestamp_utc",
                "account_type",
                "device_type",
                "difficulty",
                "level_name",
                "checkpoint",
                "checkpoint_duration",
                "deaths",
                "fails",
            ]
        )

    @staticmethod
    def from_html_row(tr) -> "BlackOps6CampaignCheckpoint":
        cells = tr.find_all("td")
        timestamp = parse_utc(cells[0].text)
        account_type = cells[1].text
        device_type = cells[2].text
        difficulty = cells[3].text
        level_name = cells[4].text
        checkpoint = cells[5].text
        checkpoint_duration = parse_float(cells[6].text)
        deaths = parse_int(cells[7].text)
        fails = parse_int(cells[8].text)

        return BlackOps6CampaignCheckpoint(
            timestamp,
            account_type,
            device_type,
            difficulty,
            level_name,
            checkpoint,
            checkpoint_duration,
            deaths,
            fails,
        )

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
