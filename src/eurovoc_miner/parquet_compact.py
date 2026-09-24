"""Parquet writer that preserves public metadata statistics, but not full-text ones."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl


def statistic_paths(schema: pa.Schema) -> list[str]:
    """Return physical Parquet leaf paths; top-level list names are insufficient."""
    paths = []
    for field in schema:
        if field.name == "text":
            continue
        dtype = field.type
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            if not (pa.types.is_string(dtype.value_type) or pa.types.is_large_string(dtype.value_type)):
                raise TypeError(f"Unsupported nested statistic column: {field.name}: {dtype}")
            paths.append(f"{field.name}.list.element")
        elif pa.types.is_struct(dtype) or pa.types.is_map(dtype):
            raise TypeError(f"Unsupported nested statistic column: {field.name}: {dtype}")
        else:
            paths.append(field.name)
    return paths


def write_compact_parquet(df, path) -> None:
    table = df.to_arrow()
    paths = statistic_paths(table.schema)
    pq.write_table(table, path, compression="zstd", version="1.0",
                   write_statistics=paths)

    source = pq.ParquetFile(path)
    if source.metadata.num_rows != table.num_rows:
        raise ValueError(f"Incomplete compact Parquet write: {path}")
    # Arrow's compliant Parquet list representation renames the child field
    # from Polars' "item" to "element". Compare the logical Polars round trip
    # instead of treating that harmless physical name change as data loss.
    restored = pl.from_arrow(source.read())
    if restored.schema != df.schema or not restored.equals(df):
        raise ValueError(f"Rows or schema changed during compact Parquet write: {path}")
    expected = set(paths)
    for group_index in range(source.metadata.num_row_groups):
        group = source.metadata.row_group(group_index)
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            has_stats = column.statistics is not None
            if has_stats != (column.path_in_schema in expected):
                raise ValueError(f"Unexpected statistics for {column.path_in_schema}: {path}")
