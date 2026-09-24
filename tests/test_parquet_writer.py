"""Compatibility checks for the weekly job's opt-in compact Parquet writer."""

from __future__ import annotations

import datetime
import hashlib
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from eurovoc_miner.cli import _safe_write_parquet
from eurovoc_miner.config import SCHEMA
from scripts.compact_hf_snapshot import rebuild


def sample_frame() -> pl.DataFrame:
    text = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(2500))
    columns = {}
    for name, dtype in SCHEMA.items():
        if dtype == pl.String:
            columns[name] = [text + " a" if name == "text" else f"{name}-a",
                             text + " b" if name == "text" else f"{name}-b"]
        else:
            columns[name] = [[f"{name}-a"], []]
    return pl.DataFrame(columns, schema=SCHEMA)


def test_compact_writer_preserves_every_value_and_schema(monkeypatch, tmp_path: Path):
    frame = sample_frame()
    baseline = tmp_path / "baseline.parquet"
    compact = tmp_path / "compact.parquet"
    frame.write_parquet(baseline)
    monkeypatch.setenv("EURLEX_COMPACT_PARQUET", "1")
    written, rows = _safe_write_parquet(frame, compact, datetime.date(2026, 1, 1))
    assert written == compact
    assert rows == len(frame)
    assert pq.read_table(compact).equals(pq.read_table(baseline), check_metadata=True)
    assert pl.read_parquet(compact).equals(frame)
    assert pq.ParquetFile(compact).metadata.format_version == "1.0"
    columns = {chunk.path_in_schema: chunk for chunk in (
        pq.ParquetFile(compact).metadata.row_group(0).column(index)
        for index in range(pq.ParquetFile(compact).metadata.row_group(0).num_columns)
    )}
    assert columns["text"].statistics is None
    assert columns["date"].statistics is not None
    assert columns["celex"].statistics is not None
    assert columns["institutions.list.element"].statistics is not None
    assert columns["eurovoc_concepts_ids.list.element"].statistics is not None
    assert compact.stat().st_size < baseline.stat().st_size * 0.9
    assert list(tmp_path.glob("*.tmp")) == []


def test_compact_writer_preserves_nonempty_on_empty_refresh(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("EURLEX_COMPACT_PARQUET", "1")
    target = tmp_path / "day.parquet"
    frame = sample_frame()
    _safe_write_parquet(frame, target, datetime.date(2026, 1, 1))
    before = target.read_bytes()
    written, rows = _safe_write_parquet(frame.head(0), target, datetime.date(2026, 1, 1))
    assert written == target
    assert rows == len(frame)
    assert target.read_bytes() == before


def test_unset_flag_keeps_polars_writer(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("EURLEX_COMPACT_PARQUET", raising=False)
    target = tmp_path / "day.parquet"
    frame = sample_frame()
    _safe_write_parquet(frame, target, datetime.date(2026, 1, 1))
    assert pq.ParquetFile(target).metadata.created_by == "Polars"


def test_optional_keyword_column_keeps_statistics(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("EURLEX_COMPACT_PARQUET", "1")
    target = tmp_path / "keyword.parquet"
    frame = sample_frame().with_columns(pl.lit(True).alias("match_copernicus"))
    _safe_write_parquet(frame, target, datetime.date(2026, 1, 1))
    columns = {pq.ParquetFile(target).metadata.row_group(0).column(index).path_in_schema:
               pq.ParquetFile(target).metadata.row_group(0).column(index)
               for index in range(pq.ParquetFile(target).metadata.row_group(0).num_columns)}
    assert columns["match_copernicus"].statistics is not None


def test_failed_compact_write_leaves_existing_file_intact(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("EURLEX_COMPACT_PARQUET", "1")
    target = tmp_path / "day.parquet"
    frame = sample_frame()
    _safe_write_parquet(frame, target, datetime.date(2026, 1, 1))
    before = target.read_bytes()

    def fail_after_partial_write(df, path):
        Path(path).write_bytes(b"partial")
        raise RuntimeError("simulated writer failure")

    import pytest
    from eurovoc_miner import parquet_compact

    monkeypatch.setattr(parquet_compact, "write_compact_parquet", fail_after_partial_write)

    with pytest.raises(RuntimeError, match="simulated writer failure"):
        _safe_write_parquet(frame, target, datetime.date(2026, 1, 1))
    assert target.read_bytes() == before
    assert list(tmp_path.glob("*.tmp")) == []


def test_offline_snapshot_rebuild_keeps_every_file_and_empty_parquet(tmp_path: Path):
    import pytest

    source = tmp_path / "source"
    folder = source / "files" / "2026"
    folder.mkdir(parents=True)
    (source / ".gitattributes").write_text("*.parquet filter=lfs\n")
    (source / "README.md").write_text("Snapshot\n")
    frame = sample_frame()
    original = folder / "nonempty.parquet"
    empty = folder / "empty.parquet"
    frame.write_parquet(original)
    frame.head(0).write_parquet(empty)
    target = tmp_path / "target"
    result = rebuild(source, target)
    assert result["files"] == 4
    assert result["parquetFiles"] == 2
    assert result["rewritten"] == 1
    assert result["emptyUnchanged"] == 1
    assert result["rows"] == len(frame)
    assert pq.read_table(target / original.relative_to(source)).equals(pq.read_table(original), check_metadata=True)
    assert (target / empty.relative_to(source)).read_bytes() == empty.read_bytes()
    assert (target / "README.md").read_bytes() == (source / "README.md").read_bytes()
    with pytest.raises(FileExistsError):
        rebuild(source, target)
