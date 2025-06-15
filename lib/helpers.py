from datetime import datetime, timezone


def parse_utc(utc: str) -> datetime:
    return datetime.strptime(utc.strip(), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )


def parse_float(f: str) -> float:
    return float(f.strip())


def parse_int(i: str) -> int:
    return int(i.strip())
