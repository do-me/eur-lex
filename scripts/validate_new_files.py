"""Validate new or refreshed daily Parquets before the weekly upload."""
import datetime
import hashlib
import json
import os
import re
from pathlib import Path

import polars as pl
from huggingface_hub import HfApi

from eurovoc_miner.config import FILES_DIR, SCHEMA


def save_seed_manifest():
    hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in Path(FILES_DIR).glob('*/dataset_*_eng.parquet')}
    Path(os.environ['EURLEX_SEED_MANIFEST']).write_text(json.dumps(hashes))


def validate(path):
    match = re.fullmatch(r'dataset_(\d{4}-\d{2}-\d{2})_eng\.parquet', path.name)
    if not match:
        raise ValueError(f'Unexpected daily filename: {path}')
    day = match.group(1)
    if path.parent.name != str(datetime.date.fromisoformat(day).year):
        raise ValueError(f'Wrong year directory: {path}')
    frame = pl.read_parquet(path)
    if dict(frame.schema) != SCHEMA:
        raise ValueError(f'Unexpected schema: {path}')
    if frame.height:
        if set(frame['date'].to_list()) != {day}:
            raise ValueError(f'Wrong document date: {path}')
        if set(frame['lang'].to_list()) != {'eng'}:
            raise ValueError(f'Wrong language: {path}')
        if frame['celex'].n_unique() != frame.height:
            raise ValueError(f'Duplicate CELEX: {path}')
        if frame.filter(pl.col('text').is_null() | (pl.col('text').str.strip_chars() == '')).height:
            raise ValueError(f'Missing document text: {path}')
    return frame.height


def main():
    api = HfApi()
    remote = set(api.list_repo_files(os.environ['HF_DATASET'], repo_type='dataset'))
    manifest = os.environ.get('EURLEX_SEED_MANIFEST')
    baseline = json.loads(Path(manifest).read_text()) if manifest else {}
    count = rows = 0
    for path in sorted(Path(FILES_DIR).glob('*/dataset_*_eng.parquet')):
        target = 'files/' + path.relative_to(FILES_DIR).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if target in remote and baseline.get(str(path)) == digest:
            continue
        n = validate(path)
        count += 1
        rows += n
        print(f'Validated {target}: {n} rows')
    print(f'Validated {count} new/refreshed files, {rows} rows')


if __name__ == '__main__':
    main()
