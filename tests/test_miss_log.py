import csv
import datetime
import tempfile
import unittest
from pathlib import Path

from eurovoc_miner.miss_log import record_attempt


class MissLogTests(unittest.TestCase):
    def test_failed_attempt_is_added_and_successful_retry_removes_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing_dates.csv"
            failed_date = datetime.date(2026, 9, 1)

            record_attempt(failed_date, "request timed out", path)
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row["date"] for row in rows], ["2026-09-01"])
            self.assertEqual(rows[0]["error"], "request timed out")

            record_attempt(failed_date, log_path=path)
            with path.open(newline="", encoding="utf-8") as handle:
                self.assertEqual(list(csv.DictReader(handle)), [])

    def test_other_unresolved_dates_are_preserved_and_sorted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing_dates.csv"
            record_attempt(datetime.date(2026, 9, 3), "third", path)
            record_attempt(datetime.date(2026, 9, 1), "first", path)

            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(
                [row["date"] for row in rows],
                ["2026-09-01", "2026-09-03"],
            )


if __name__ == "__main__":
    unittest.main()
