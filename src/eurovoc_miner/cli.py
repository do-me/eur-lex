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
    parser.add_argument('--days', type=int, default=1, help='Number of days to look back')
    parser.add_argument('--lang', type=str, help='Filter by language code (e.g. ENG, SPA, FRA)')
    parser.add_argument('--keywords', nargs='+', help='Optional keywords to match in full text')
    parser.add_argument('--save-only-keyword-matches', action='store_true', help='Only save records that match at least one keyword')
    args = parser.parse_args()

    lang_filter = args.lang.upper() if args.lang else None
    
    for i in range(args.days):
        date = datetime.date.today() - datetime.timedelta(days=i)
        
        try:
            docs = list(get_docs_text(date, lang=lang_filter))
            
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
                
                if args.save_only_keyword_matches and args.keywords:
                    df = filter_keyword_matches(df, args.keywords)
                    log.info(f"Filtered to {len(df)} keyword matches.")

            lang_suffix = f"_{args.lang.lower()}" if args.lang else ""
            filename = f"{args.output_prefix}{date}{lang_suffix}.parquet"
            output_path = os.path.join(FILES_DIR, filename)
            
            df.write_parquet(output_path)
            if not docs:
                log.info(f"✓ Saved empty file to {output_path}")
            else:
                log.info(f"✓ Saved {len(df)} records to {output_path}")

        except Exception as e:
            log.error(f"Failed to process {date}: {e}")

if __name__ == "__main__":
    run()
