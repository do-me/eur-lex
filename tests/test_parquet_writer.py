"""Compatibility checks for the weekly job's opt-in compact Parquet writer."""

from __future__ import annotations

import datetime
import hashlib
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from eurovoc_miner.cli import _safe_write_parquet
from eurovoc_miner.config import SCHEMA


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
    assert columns["date"].statistics is None
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


def test_failed_compact_write_leaves_existing_file_intact(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("EURLEX_COMPACT_PARQUET", "1")
    target = tmp_path / "day.parquet"
    frame = sample_frame()
    _safe_write_parquet(frame, target, datetime.date(2026, 1, 1))
    before = target.read_bytes()

    class FailingFrame:
        def __len__(self):
            return len(frame)

        def write_parquet(self, path, **kwargs):
            Path(path).write_bytes(b"partial")
            raise RuntimeError("simulated writer failure")

    import pytest

    with pytest.raises(RuntimeError, match="simulated writer failure"):
        _safe_write_parquet(FailingFrame(), target, datetime.date(2026, 1, 1))
    assert target.read_bytes() == before
    assert list(tmp_path.glob("*.tmp")) == []
