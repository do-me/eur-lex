import datetime
import unittest
from argparse import Namespace
from unittest.mock import patch

from eurovoc_miner.cli import _run_backfill


class LoggedRetryTests(unittest.TestCase):
    @patch('eurovoc_miner.cli.record_attempt')
    @patch('eurovoc_miner.cli._mine_one_day', return_value=('rows', 2, 'file.parquet'))
    @patch('eurovoc_miner.cli.failed_dates', return_value={'2026-08-25'})
    @patch('eurovoc_miner.cli._scan_existing', return_value={'2026-08-25': ('file.parquet', 1)})
    def test_failed_refresh_is_retried_despite_existing_rows(self, scan, dates, mine, record):
        args = Namespace(days_per_request=1, to_date='2026-08-25', from_date='2026-08-25',
                         retry_empty=False, retry_logged=True, output_prefix='dataset_',
                         dry_run=False, keywords=None, save_only_keyword_matches=False,
                         unique_on='celex')
        _run_backfill(args, 'ENG', '_eng')
        mine.assert_called_once()
        record.assert_called_once_with(datetime.date(2026, 8, 25))
        args.retry_logged = False
        mine.reset_mock()
        _run_backfill(args, 'ENG', '_eng')
        mine.assert_not_called()
