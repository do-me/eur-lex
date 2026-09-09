import csv
import datetime
import os
from pathlib import Path

DEFAULT_LOG_PATH = Path(__file__).resolve().parents[2] / "missing_dates.csv"


def record_attempt(date, error=None, log_path=None):
    """Keep a CSV containing only dates whose latest write attempt failed."""
    path = Path(log_path or os.environ.get("EUR_LEX_MISS_LOG", DEFAULT_LOG_PATH))
    rows = {}

    if path.exists():
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("date"):
                    rows[row["date"]] = row

    date_string = date.isoformat()
    if error is None:
        rows.pop(date_string, None)
    else:
        rows[date_string] = {
            "date": date_string,
            "last_attempt_utc": datetime.datetime.now(datetime.UTC).replace(
                microsecond=0
            ).isoformat(),
            "error": " ".join(str(error).split())[:500],
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["date", "last_attempt_utc", "error"]
        )
        writer.writeheader()
        writer.writerows(rows[key] for key in sorted(rows))
    temporary_path.replace(path)
