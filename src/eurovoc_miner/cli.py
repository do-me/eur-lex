import argparse
import datetime
import os
import re
import polars as pl
import logging
from .core import get_docs_text
from .processor import clean_text_batch, match_keywords, filter_keyword_matches
from .config import FILES_DIR, SCHEMA

log = logging.getLogger(__name__)


def _mine_one_day(date, output_prefix, lang_filter, lang_suffix, keywords,
                  save_only_kw, unique_on):
    """Mine a single date and write its parquet. Returns ('rows'|'empty', n_rows, path).

    Mirrors the original per-day code path (days=1) so backfilled files are
    schema-identical to weekly-run output.
    """
    docs = list(get_docs_text(date, lang=lang_filter, days=1))
    if not docs:
        df = pl.DataFrame([], schema=SCHEMA)
        df = match_keywords(df, keywords)
        status = 'empty'
    else:
        df = pl.DataFrame(docs, schema=SCHEMA)
        df = clean_text_batch(df)
        df = match_keywords(df, keywords)
        if unique_on:
            if unique_on in df.columns:
                original_len = len(df)
                df = df.unique(subset=[unique_on], keep='last')
                if len(df) < original_len:
                    log.info(f"Deduplicated: {original_len} -> {len(df)} (unique on {unique_on})")
            else:
                log.warning(f"Column '{unique_on}' not found for deduplication. Available: {df.columns}")
        if save_only_kw and keywords:
            df = filter_keyword_matches(df, keywords)
            log.info(f"Filtered to {len(df)} keyword matches.")
        status = 'rows'

    filename = f"{output_prefix}{date}{lang_suffix}.parquet"
    year_dir = os.path.join(FILES_DIR, str(date.year))
    os.makedirs(year_dir, exist_ok=True)
    output_path = os.path.join(year_dir, filename)
    df.write_parquet(output_path)
    return status, len(df), output_path


def _scan_existing(output_prefix, lang_suffix):
    """Return {iso_date: (path, num_rows)} for matching parquets under FILES_DIR.

    Walks recursively so files/2026/dataset_2026-04-01_eng.parquet is found
    even though the miner writes flat. num_rows comes from parquet metadata
    only (no payload load). Files whose metadata cannot be read are treated
    as missing (None).
    """
    import pyarrow.parquet as pq
    suffix = f"{lang_suffix}.parquet"
    out = {}
    if not os.path.isdir(FILES_DIR):
        return out
    for root, _dirs, files in os.walk(FILES_DIR):
        for entry in files:
            if not entry.startswith(output_prefix) or not entry.endswith(suffix):
                continue
            stem = entry[len(output_prefix):-len(suffix)]
            # Per-day filenames only. Skip date-range files produced by --days-per-request>1.
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", stem):
                continue
            full = os.path.join(root, entry)
            try:
                nrows = pq.ParquetFile(full).metadata.num_rows
            except Exception as exc:
                log.warning(f"Could not read parquet metadata for {full}: {exc}")
                nrows = None
            # If the same date appears in multiple locations (flat + year-subdir),
            # prefer the one with the most rows so re-scans don't churn.
            prev = out.get(stem)
            if prev is None or (nrows is not None and (prev[1] is None or nrows > prev[1])):
                out[stem] = (full, nrows)
    return out


def _parse_iso(s, flag_name):
    try:
        return datetime.date.fromisoformat(s)
    except ValueError as exc:
        raise SystemExit(f"Invalid {flag_name} '{s}': expected YYYY-MM-DD ({exc})")


def _run_backfill(args, lang_filter, lang_suffix):
    if args.days_per_request != 1:
        raise SystemExit("--backfill-missing requires --days-per-request 1 "
                         "(per-day filenames are needed to detect gaps).")

    today = datetime.date.today()
    to_date = _parse_iso(args.to_date, '--to-date') if args.to_date else today
    if args.from_date:
        from_date = _parse_iso(args.from_date, '--from-date')
    else:
        if args.retry_empty:
            raise SystemExit("--retry-empty requires an explicit --from-date "
                             "(refusing to re-mine every empty file back to 1973).")
        from_date = None  # resolved below from existing files

    existing = _scan_existing(args.output_prefix, lang_suffix)
    if from_date is None:
        if not existing:
            raise SystemExit("No existing files found and no --from-date given; nothing to backfill.")
        from_date = min(datetime.date.fromisoformat(d) for d in existing)

    if to_date < from_date:
        raise SystemExit(f"--to-date {to_date} is before --from-date {from_date}.")
    if to_date > today:
        log.warning(f"--to-date {to_date} is in the future; clamping to today ({today}).")
        to_date = today

    to_fetch = []  # list of (date, reason)
    skipped_present = 0
    skipped_empty = 0
    d = from_date
    while d <= to_date:
        iso = d.isoformat()
        rec = existing.get(iso)
        if rec is None:
            to_fetch.append((d, 'missing'))
        else:
            _path, nrows = rec
            if nrows == 0:
                if args.retry_empty:
                    to_fetch.append((d, 'empty'))
                else:
                    skipped_empty += 1
            else:
                skipped_present += 1
        d += datetime.timedelta(days=1)

    n_total = (to_date - from_date).days + 1
    log.info(f"Backfill scan: {from_date} -> {to_date} ({n_total} days). "
             f"To fetch: {len(to_fetch)} (missing={sum(1 for _, r in to_fetch if r == 'missing')}, "
             f"empty-retry={sum(1 for _, r in to_fetch if r == 'empty')}). "
             f"Skipped: {skipped_present} with rows, {skipped_empty} empty (use --retry-empty to include).")

    if args.dry_run:
        for date, reason in to_fetch:
            log.info(f"DRY-RUN would fetch {date}  [{reason}]")
        return

    if not to_fetch:
        log.info("Nothing to do.")
        return

    summary = {'rows': 0, 'empty': 0, 'errors': 0}
    fetched_with_rows = []
    fetched_empty = []
    errors = []
    for date, reason in to_fetch:
        try:
            status, n, path = _mine_one_day(
                date, args.output_prefix, lang_filter, lang_suffix,
                args.keywords, args.save_only_keyword_matches, args.unique_on)
            summary[status] += 1
            if status == 'rows':
                fetched_with_rows.append((date, n, path))
                log.info(f"✓ Backfilled {date} [{reason}] -> {n} records ({path})")
            else:
                fetched_empty.append((date, path))
                log.info(f"∅ Backfilled {date} [{reason}] -> empty (SPARQL returned 0 docs)")
        except Exception as exc:
            summary['errors'] += 1
            errors.append((date, repr(exc)))
            log.error(f"✗ Failed to backfill {date} [{reason}]: {exc}")

    log.info("=" * 60)
    log.info(f"Backfill summary ({from_date} -> {to_date}):")
    log.info(f"  Attempted   : {len(to_fetch)}")
    log.info(f"  With rows   : {summary['rows']}")
    log.info(f"  Still empty : {summary['empty']}")
    log.info(f"  Errors      : {summary['errors']}")
    if errors:
        log.info("  Errored dates:")
        for date, msg in errors:
            log.info(f"    {date}: {msg}")
    if fetched_empty:
        log.info("  Empty-after-fetch dates (genuine zero-publication days or persistent API trouble):")
        for date, _path in fetched_empty:
            log.info(f"    {date}")
    log.info("=" * 60)


def _run_lookback(args, lang_filter, lang_suffix):
    total_days = args.lookback if args.lookback is not None else args.days

    for i in range(0, total_days, args.days_per_request):
        current_batch_days = min(args.days_per_request, total_days - i)
        date = datetime.date.today() - datetime.timedelta(days=i + current_batch_days - 1)

        try:
            docs = list(get_docs_text(date, lang=lang_filter, days=current_batch_days))

            if not docs:
                df = pl.DataFrame([], schema=SCHEMA)
                df = match_keywords(df, args.keywords)
                log.info(f"∅ No documents for {date}, creating empty file.")
            else:
                df = pl.DataFrame(docs, schema=SCHEMA)
                df = clean_text_batch(df)
                df = match_keywords(df, args.keywords)

                if args.unique_on:
                    if args.unique_on in df.columns:
                        original_len = len(df)
                        df = df.unique(subset=[args.unique_on], keep="last")
                        if len(df) < original_len:
                            log.info(f"Deduplicated: {original_len} -> {len(df)} records (unique on {args.unique_on})")
                    else:
                        log.warning(f"Column '{args.unique_on}' not found for deduplication. Available: {df.columns}")

                if args.save_only_keyword_matches and args.keywords:
                    df = filter_keyword_matches(df, args.keywords)
                    log.info(f"Filtered to {len(df)} keyword matches.")

            if current_batch_days > 1:
                end_date = date + datetime.timedelta(days=current_batch_days - 1)
                filename = f"{args.output_prefix}{date}_to_{end_date}{lang_suffix}.parquet"
            else:
                filename = f"{args.output_prefix}{date}{lang_suffix}.parquet"

            # Place in year-subdir so local layout mirrors the HF dataset
            # (files/YYYY/dataset_YYYY-MM-DD_eng.parquet). The start date's year
            # is used for batched range files.
            year_dir = os.path.join(FILES_DIR, str(date.year))
            os.makedirs(year_dir, exist_ok=True)
            output_path = os.path.join(year_dir, filename)

            df.write_parquet(output_path)
            if not docs:
                log.info(f"✓ Saved empty file to {output_path}")
            else:
                log.info(f"✓ Saved {len(df)} records to {output_path}")

        except Exception as e:
            batch_desc = f"{date}" if current_batch_days == 1 else f"{date} to {date + datetime.timedelta(days=current_batch_days-1)}"
            log.error(f"Failed to process {batch_desc}: {e}")


def run():
    parser = argparse.ArgumentParser(description='Eurovoc Miner - European Commission Cellar Data Extraction')
    parser.add_argument('output_prefix', type=str, help='Prefix for the output Parquet files')
    parser.add_argument('--days', type=int, default=1, help='Number of days to process (alias for --lookback)')
    parser.add_argument('--lookback', type=int, help='Safety margin: how many days into the past to search')
    parser.add_argument('--lang', type=str, help='Filter by language code (e.g. ENG, SPA, FRA)')
    parser.add_argument('--keywords', nargs='+', help='Optional keywords to match in full text')
    parser.add_argument('--save-only-keyword-matches', action='store_true', help='Only save records that match at least one of the provided keywords')
    parser.add_argument('--days-per-request', type=int, default=1, help='Number of days to fetch in a single SPARQL request')
    parser.add_argument('--unique-on', type=str, help='Column name to ensure uniqueness (e.g. celex)')

    bf = parser.add_argument_group('backfill', 'Intelligent backfill: re-mine only dates with no parquet (or empty parquets) in a range.')
    bf.add_argument('--backfill-missing', action='store_true',
                    help='Enable backfill mode. Scans FILES_DIR for the given prefix/lang and re-fetches only the dates that are missing within the range.')
    bf.add_argument('--retry-empty', action='store_true',
                    help='In backfill mode, also re-fetch dates whose existing parquet has 0 rows. Requires --from-date.')
    bf.add_argument('--from-date', type=str, help='Backfill range start (YYYY-MM-DD). Default: earliest existing matching date.')
    bf.add_argument('--to-date', type=str, help='Backfill range end (YYYY-MM-DD). Default: today.')
    bf.add_argument('--dry-run', action='store_true', help='Report what would be backfilled; no SPARQL/document fetches.')

    args = parser.parse_args()

    lang_filter = args.lang.upper() if args.lang else None
    lang_suffix = f"_{args.lang.lower()}" if args.lang else ""

    if args.backfill_missing:
        _run_backfill(args, lang_filter, lang_suffix)
    else:
        _run_lookback(args, lang_filter, lang_suffix)


if __name__ == "__main__":
    run()
