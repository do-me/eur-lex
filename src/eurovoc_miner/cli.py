import argparse
import datetime
import os
import polars as pl
import logging
from .core import get_docs_text
from .processor import clean_text_batch, match_keywords, filter_keyword_matches
from .config import FILES_DIR, SCHEMA

log = logging.getLogger(__name__)

def run():
    parser = argparse.ArgumentParser(description='Eurovoc Miner - European Commission Cellar Data Extraction')
    parser.add_argument('output_prefix', type=str, help='Prefix for the output Parquet files')
    parser.add_argument('--days', type=int, default=1, help='Number of days to process (alias for --lookback)')
    parser.add_argument('--lookback', type=int, help='Safety margin: how many days into the past to search')
    parser.add_argument('--lang', type=str, help='Filter by language code (e.g. ENG, SPA, FRA)')
    parser.add_argument('--keywords', nargs='+', help='Optional keywords to match in full text')
    parser.add_argument('--save-only-keyword-matches', action='store_true', help='Only save records that match at least one keyword')
    parser.add_argument('--days-per-request', type=int, default=1, help='Number of days to fetch in a single SPARQL request')
    parser.add_argument('--unique-on', type=str, help='Column name to ensure uniqueness (e.g. celex)')
    args = parser.parse_args()

    # Use lookback as window size if provided
    total_days = args.lookback if args.lookback is not None else args.days
    lang_filter = args.lang.upper() if args.lang else None
    
    # Process in batches of days_per_request
    for i in range(0, total_days, args.days_per_request):
        # Calculate how many days to fetch in this specific batch (handles remainder)
        current_batch_days = min(args.days_per_request, total_days - i)
        
        # d is the "oldest" date in the batch (start of the range)
        # Because we go backwards from today, d = today - i - (batch_size - 1)
        # i.e. if i=0, batch=60, range is from [today-59] to [today]
        # Start date for SPARQL (oldest)
        date = datetime.date.today() - datetime.timedelta(days=i + current_batch_days - 1)
        
        try:
            docs = list(get_docs_text(date, lang=lang_filter, days=current_batch_days))
            
            if not docs:
                # Create empty DataFrame with schema
                df = pl.DataFrame([], schema=SCHEMA)
                # Still add keyword columns to empty DF to keep schema consistent
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

            lang_suffix = f"_{args.lang.lower()}" if args.lang else ""
            
            # Use a date range in the filename if batching
            if current_batch_days > 1:
                end_date = date + datetime.timedelta(days=current_batch_days - 1)
                filename = f"{args.output_prefix}{date}_to_{end_date}{lang_suffix}.parquet"
            else:
                filename = f"{args.output_prefix}{date}{lang_suffix}.parquet"
                
            output_path = os.path.join(FILES_DIR, filename)
            
            df.write_parquet(output_path)
            if not docs:
                log.info(f"✓ Saved empty file to {output_path}")
            else:
                log.info(f"✓ Saved {len(df)} records to {output_path}")

        except Exception as e:
            # More descriptive error for batches
            batch_desc = f"{date}" if current_batch_days == 1 else f"{date} to {date + datetime.timedelta(days=current_batch_days-1)}"
            log.error(f"Failed to process {batch_desc}: {e}")

if __name__ == "__main__":
    run()
