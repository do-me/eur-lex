# /// script
# dependencies = [
#   "huggingface_hub",
#   "pandas",
# ]
# ///

from huggingface_hub import HfApi
import pandas as pd
import re

REPO_ID = "do-me/EUR-LEX"
api = HfApi()

print(f"Fetching file list from {REPO_ID}...")
all_files = api.list_repo_files(REPO_ID, repo_type="dataset")

# The actual pattern: files/1973/dataset_1973-01-14_eng.parquet
# We match 'dataset_' followed by 'YYYY-MM-DD'
date_pattern = re.compile(r"dataset_(\d{4}-\d{2}-\d{2})")

date_strings = []
for f in all_files:
    match = date_pattern.search(f)
    if match:
        date_strings.append(match.group(1))

if not date_strings:
    print("Failed to find date strings. Pattern mismatch.")
    print("Example filename found:", next((f for f in all_files if ".parquet" in f), "None"))
else:
    # Convert to unique dates
    found_dates = pd.to_datetime(date_strings, format='%Y-%m-%d').unique()
    found_dates = sorted(found_dates)
    
    start_date = found_dates[0]
    end_date = found_dates[-1]
    
    # Create a full range of dates
    full_range = pd.date_range(start=start_date, end=end_date)
    
    # Find missing
    missing = full_range.difference(found_dates)
    
    print(f"\nRange analyzed: {start_date.date()} to {end_date.date()}")
    print(f"Unique days with data: {len(found_dates)}")
    print(f"Total gaps in calendar: {len(missing)}")
    
    if len(missing) > 0:
        print("\nFirst 20 missing dates:")
        for d in missing[:20]:
            print(d.date())
        if len(missing) > 20:
            print(f"... and {len(missing) - 20} more.")
    else:
        print("\nSuccess: No missing dates found in the range!")

"""
uv run check.py
Reading inline script metadata from `check.py`
Fetching file list from do-me/EUR-LEX...

Range analyzed: 1973-01-14 to 2026-02-16
Unique days with data: 19392
Total gaps in calendar: 0

Success: No missing dates found in the range!
"""
