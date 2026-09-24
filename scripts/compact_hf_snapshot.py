"""Rebuild a verified local HF snapshot, omitting only full-text statistics.

This script never contacts or writes to the Hub. SOURCE must be a clean pinned
snapshot; OUTPUT must not exist. An upload is a separate, deliberate step.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from eurovoc_miner.parquet_compact import write_compact_parquet


def rebuild(source: Path, output: Path) -> dict:
    source = source.resolve(strict=True)
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"Output must not exist: {output}")
    if source == output or source in output.parents or output in source.parents:
        raise ValueError("Source and output must be separate directories")
    if not (source / "files").is_dir() or not (source / ".gitattributes").is_file():
        raise ValueError("Source is not an EUR-LEX Hub snapshot")

    paths = sorted(path for path in source.rglob("*") if path.is_file()
                   and ".cache" not in path.relative_to(source).parts)
    if not paths:
        raise ValueError("Source snapshot is empty")
    started = time.perf_counter()
    output.mkdir(parents=True)
    summary = {"files": 0, "parquetFiles": 0, "rewritten": 0, "emptyUnchanged": 0,
               "rows": 0, "oldBytes": 0, "newBytes": 0,
               "oldFooterBytes": 0, "newFooterBytes": 0}
    for source_path in paths:
        relative = source_path.relative_to(source)
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        summary["files"] += 1
        summary["oldBytes"] += source_path.stat().st_size

        if source_path.suffix == ".parquet":
            old_file = pq.ParquetFile(source_path)
            old_meta = old_file.metadata
            summary["parquetFiles"] += 1
            summary["rows"] += old_meta.num_rows
            summary["oldFooterBytes"] += old_meta.serialized_size
            if old_meta.num_rows == 0:
                shutil.copy2(source_path, target)
                summary["emptyUnchanged"] += 1
            else:
                frame = pl.read_parquet(source_path)
                write_compact_parquet(frame, target)
                original = old_file.read()
                rewritten = pq.read_table(target)
                if not rewritten.equals(original, check_metadata=True):
                    raise ValueError(f"Rows or Arrow schema changed: {relative}")
                new_meta = pq.ParquetFile(target).metadata
                if new_meta.num_row_groups != old_meta.num_row_groups:
                    raise ValueError(f"Row-group count changed: {relative}")
                for group_index in range(new_meta.num_row_groups):
                    old_group = old_meta.row_group(group_index)
                    new_group = new_meta.row_group(group_index)
                    if old_group.num_columns != new_group.num_columns:
                        raise ValueError(f"Column count changed: {relative}")
                    for column_index in range(new_group.num_columns):
                        old_column = old_group.column(column_index)
                        new_column = new_group.column(column_index)
                        name = new_column.path_in_schema
                        if name != old_column.path_in_schema:
                            raise ValueError(f"Parquet leaf path changed: {relative}: {name}")
                        if name == "text":
                            if new_column.statistics is not None:
                                raise ValueError(f"Text statistics retained: {relative}")
                        elif old_column.statistics is not None and new_column.statistics is None:
                            raise ValueError(f"Metadata statistics dropped: {relative}: {name}")
                summary["rewritten"] += 1
            summary["newFooterBytes"] += pq.ParquetFile(target).metadata.serialized_size
        else:
            shutil.copy2(source_path, target)
            if target.read_bytes() != source_path.read_bytes():
                raise ValueError(f"Non-Parquet file changed: {relative}")
        summary["newBytes"] += target.stat().st_size
        if summary["parquetFiles"] and summary["parquetFiles"] % 2500 == 0:
            print(json.dumps({"event": "progress", "parquetFiles": summary["parquetFiles"],
                              "elapsedSeconds": round(time.perf_counter() - started, 1)}), flush=True)

    produced = {path.relative_to(output).as_posix() for path in output.rglob("*") if path.is_file()}
    expected = {path.relative_to(source).as_posix() for path in paths}
    if produced != expected:
        raise ValueError("Output file inventory differs from source")
    summary["savedPercent"] = round(
        100 * (summary["oldBytes"] - summary["newBytes"]) / summary["oldBytes"], 2)
    summary["elapsedSeconds"] = round(time.perf_counter() - started, 1)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(rebuild(args.source, args.output), indent=2), flush=True)


if __name__ == "__main__":
    main()
