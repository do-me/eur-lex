import tempfile
import unittest
from pathlib import Path

import polars as pl

from eurovoc_miner.config import SCHEMA
from scripts.validate_new_files import validate


class BackfillValidationTests(unittest.TestCase):
    def frame(self):
        row = {key: ([] if isinstance(dtype, pl.List) else '') for key, dtype in SCHEMA.items()}
        row.update(date='2026-05-31', lang='eng', celex='32026R0001', text='Document text')
        return pl.DataFrame([row], schema=SCHEMA)

    def check(self, frame):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / '2026' / 'dataset_2026-05-31_eng.parquet'
            path.parent.mkdir()
            frame.write_parquet(path)
            return validate(path)

    def test_valid_rows_and_empty_days(self):
        self.assertEqual(self.check(self.frame()), 1)
        self.assertEqual(self.check(pl.DataFrame([], schema=SCHEMA)), 0)

    def test_wrong_date_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Wrong document date'):
            self.check(self.frame().with_columns(pl.lit('2026-06-01').alias('date')))

    def test_duplicate_celex_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate CELEX'):
            self.check(pl.concat([self.frame(), self.frame()]))

    def test_blank_text_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Missing document text'):
            self.check(self.frame().with_columns(pl.lit(' ').alias('text')))
