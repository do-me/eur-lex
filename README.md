# EUR-LEX Miner 🇪🇺

A high-performance mining tool for extracting text and semantic concepts from the European Commission's Cellar database.

## Features
- **Fast SPARQL Retrieval**: Custom J2-templated queries for efficient metadata fetching.
- **Parallel Parsing**: Multi-threaded extraction from PDF, DOCX, and HTML.
- **Vectorized Preprocessing**: Ultra-fast text cleaning powered by `Polars`.
- **Modular Design**: Clean separation of concerns following modern Python package standards.
- **Robust Caching**: Joblib-powered caching to avoid redundant downloads and expensive parsing.
- **Batch Discovery**: Tweak network efficiency with the `--days-per-request` flag to fetch metadata for multiple days in one SPARQL call.
- **Rich Metadata Collection**: Automatically extracts:
  - CELEX numbers for legal uniquely indexing.
  - Institution/Author labels (e.g., "European Commission").
  - Procedure IDs for legislative tracking.
  - Document types and Eurovoc concept IDs.

## Installation
Ensure you have `uv` installed, then:
```bash
git clone <repo-url>
cd Eurovoc_2025
uv sync
```

## Usage
Run the miner using the CLI entry point:
```bash
# General usage for all languages
uv run eurovoc-miner dataset_ --days 10

# Specific language filtering
uv run eurovoc-miner dataset_ --days 5 --lang ENG

# Keyword Matching
# Add boolean columns for specific terms (case-insensitive)
uv run eurovoc-miner dataset_ --days 5 --lang ENG --keywords "earth observation" copernicus

# Batch Discovery
# Fetch 60 days of metadata in a single SPARQL request (faster for large spans)
uv run eurovoc-miner dataset_ --days 60 --days-per-request 60
```

Large batches might give you a slight performance boost, but probably irrelevant for most use cases. Comparison: 

<details>
<summary>Past 30 days, 30 requests (40.079s) vs 1 request (38.349s)</summary>

```zsh
(base) ➜  Eurovoc_2025 git:(main) time uv run eurovoc-miner dataset_history --days 30 --days-per-request 30 --lang ENG
2026-01-28 12:38:37,533 - eurovoc_miner.core - INFO - Processing 333 documents for 2025-12-30 to 2026-01-28
2025-12-30 to 2026-01-28: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 333/333 [00:12<00:00, 26.51it/s]
2026-01-28 12:38:50,320 - eurovoc_miner.cli - INFO - ✓ Saved 303 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_history2025-12-30_to_2026-01-28_eng.parquet
uv run eurovoc-miner dataset_history --days 30 --days-per-request 30 --lang   40.44s user 1.68s system 109% cpu 38.349 total
(base) ➜  Eurovoc_2025 git:(main) time uv run eurovoc-miner dataset_ --days 30 --days-per-request 1 --lang ENG 
2026-01-28 12:39:35,265 - eurovoc_miner.core - INFO - Processing 1 documents for 2026-01-28
2026-01-28: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  6.20it/s]
2026-01-28 12:39:35,463 - eurovoc_miner.cli - INFO - ✓ Saved 1 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-28_eng.parquet
2026-01-28 12:39:36,307 - eurovoc_miner.core - INFO - Processing 7 documents for 2026-01-27
2026-01-27: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 7/7 [00:00<00:00, 37.39it/s]
2026-01-28 12:39:36,562 - eurovoc_miner.cli - INFO - ✓ Saved 7 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-27_eng.parquet
2026-01-28 12:39:38,917 - eurovoc_miner.core - INFO - Processing 33 documents for 2026-01-26
2026-01-26: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 33/33 [00:00<00:00, 145.75it/s]
2026-01-28 12:39:39,305 - eurovoc_miner.cli - INFO - ✓ Saved 33 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-26_eng.parquet
2026-01-28 12:39:39,699 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-25, creating empty file.
2026-01-28 12:39:39,700 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-25_eng.parquet
2026-01-28 12:39:40,177 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-24, creating empty file.
2026-01-28 12:39:40,180 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-24_eng.parquet
2026-01-28 12:39:42,279 - eurovoc_miner.core - INFO - Processing 28 documents for 2026-01-23
2026-01-23: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 28/28 [00:00<00:00, 118.97it/s]
2026-01-28 12:39:42,664 - eurovoc_miner.cli - INFO - ✓ Saved 23 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-23_eng.parquet
2026-01-28 12:39:44,256 - eurovoc_miner.core - INFO - Processing 18 documents for 2026-01-22
2026-01-22: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 18/18 [00:00<00:00, 75.58it/s]
2026-01-28 12:39:44,647 - eurovoc_miner.cli - INFO - ✓ Saved 15 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-22_eng.parquet
2026-01-28 12:39:47,137 - eurovoc_miner.core - INFO - Processing 36 documents for 2026-01-21
2026-01-21: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 36/36 [00:00<00:00, 153.10it/s]
2026-01-28 12:39:47,522 - eurovoc_miner.cli - INFO - ✓ Saved 33 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-21_eng.parquet
2026-01-28 12:39:49,706 - eurovoc_miner.core - INFO - Processing 31 documents for 2026-01-20
2026-01-20: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 31/31 [00:00<00:00, 135.42it/s]
2026-01-28 12:39:50,112 - eurovoc_miner.cli - INFO - ✓ Saved 29 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-20_eng.parquet
2026-01-28 12:39:51,489 - eurovoc_miner.core - INFO - Processing 16 documents for 2026-01-19
2026-01-19: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 16/16 [00:00<00:00, 71.30it/s]
2026-01-28 12:39:51,866 - eurovoc_miner.cli - INFO - ✓ Saved 15 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-19_eng.parquet
2026-01-28 12:39:52,360 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-18, creating empty file.
2026-01-28 12:39:52,361 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-18_eng.parquet
2026-01-28 12:39:52,878 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-17, creating empty file.
2026-01-28 12:39:52,879 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-17_eng.parquet
2026-01-28 12:39:54,190 - eurovoc_miner.core - INFO - Processing 15 documents for 2026-01-16
2026-01-16: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 15/15 [00:00<00:00, 69.88it/s]
2026-01-28 12:39:54,552 - eurovoc_miner.cli - INFO - ✓ Saved 13 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-16_eng.parquet
2026-01-28 12:39:56,940 - eurovoc_miner.core - INFO - Processing 33 documents for 2026-01-15
2026-01-15: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 33/33 [00:00<00:00, 135.13it/s]
2026-01-28 12:39:57,350 - eurovoc_miner.cli - INFO - ✓ Saved 29 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-15_eng.parquet
2026-01-28 12:39:59,039 - eurovoc_miner.core - INFO - Processing 21 documents for 2026-01-14
2026-01-14: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 21/21 [00:00<00:00, 94.03it/s]
2026-01-28 12:39:59,419 - eurovoc_miner.cli - INFO - ✓ Saved 18 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-14_eng.parquet
2026-01-28 12:40:00,748 - eurovoc_miner.core - INFO - Processing 15 documents for 2026-01-13
2026-01-13: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 15/15 [00:00<00:00, 69.36it/s]
2026-01-28 12:40:01,142 - eurovoc_miner.cli - INFO - ✓ Saved 14 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-13_eng.parquet
2026-01-28 12:40:03,223 - eurovoc_miner.core - INFO - Processing 24 documents for 2026-01-12
2026-01-12: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 24/24 [00:00<00:00, 76.47it/s]
2026-01-28 12:40:03,763 - eurovoc_miner.cli - INFO - ✓ Saved 23 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-12_eng.parquet
2026-01-28 12:40:04,431 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-11, creating empty file.
2026-01-28 12:40:04,432 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-11_eng.parquet
2026-01-28 12:40:04,957 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-10, creating empty file.
2026-01-28 12:40:04,958 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-10_eng.parquet
2026-01-28 12:40:06,618 - eurovoc_miner.core - INFO - Processing 17 documents for 2026-01-09
2026-01-09: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 17/17 [00:00<00:00, 71.38it/s]
2026-01-28 12:40:07,024 - eurovoc_miner.cli - INFO - ✓ Saved 15 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-09_eng.parquet
2026-01-28 12:40:07,785 - eurovoc_miner.core - INFO - Processing 7 documents for 2026-01-08
2026-01-08: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 7/7 [00:00<00:00, 39.67it/s]
2026-01-28 12:40:08,036 - eurovoc_miner.cli - INFO - ✓ Saved 7 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-08_eng.parquet
2026-01-28 12:40:08,620 - eurovoc_miner.core - INFO - Processing 3 documents for 2026-01-07
2026-01-07: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 3/3 [00:00<00:00, 17.43it/s]
2026-01-28 12:40:08,852 - eurovoc_miner.cli - INFO - ✓ Saved 2 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-07_eng.parquet
2026-01-28 12:40:09,884 - eurovoc_miner.core - INFO - Processing 9 documents for 2026-01-06
2026-01-06: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 9/9 [00:00<00:00, 50.34it/s]
2026-01-28 12:40:10,164 - eurovoc_miner.cli - INFO - ✓ Saved 8 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-06_eng.parquet
2026-01-28 12:40:10,903 - eurovoc_miner.core - INFO - Processing 7 documents for 2026-01-05
2026-01-05: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 7/7 [00:00<00:00, 39.85it/s]
2026-01-28 12:40:11,156 - eurovoc_miner.cli - INFO - ✓ Saved 6 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-05_eng.parquet
2026-01-28 12:40:11,584 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-04, creating empty file.
2026-01-28 12:40:11,585 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-04_eng.parquet
2026-01-28 12:40:11,911 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-03, creating empty file.
2026-01-28 12:40:11,912 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-03_eng.parquet
2026-01-28 12:40:12,313 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-02, creating empty file.
2026-01-28 12:40:12,314 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-02_eng.parquet
2026-01-28 12:40:12,784 - eurovoc_miner.cli - INFO - ∅ No documents for 2026-01-01, creating empty file.
2026-01-28 12:40:12,786 - eurovoc_miner.cli - INFO - ✓ Saved empty file to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2026-01-01_eng.parquet
2026-01-28 12:40:13,390 - eurovoc_miner.core - INFO - Processing 5 documents for 2025-12-31
2025-12-31: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 5/5 [00:00<00:00, 28.90it/s]
2026-01-28 12:40:13,631 - eurovoc_miner.cli - INFO - ✓ Saved 5 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2025-12-31_eng.parquet
2026-01-28 12:40:14,318 - eurovoc_miner.core - INFO - Processing 7 documents for 2025-12-30
2025-12-30: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 7/7 [00:00<00:00, 40.57it/s]
2026-01-28 12:40:14,568 - eurovoc_miner.cli - INFO - ✓ Saved 7 records to /Users/dome/work/jrc/Eurovoc_2025/files/dataset_2025-12-30_eng.parquet
uv run eurovoc-miner dataset_ --days 30 --days-per-request 1 --lang ENG  56.75s user 15.49s system 180% cpu 40.079 total
```

</details>


### Keyword Matching Logic
When using the `--keywords` flag:
- **Compound Terms**: Wrap terms with spaces in quotes (e.g., `"earth observation"`).
- **Auto-Sanitization**: Keywords are converted to clean snake_case column names (e.g., `match_earth_observation`).
- **Performance**: Matching is performed using vectorized Polars operations.
- **Consistency**: Empty daily files still contain the keyword columns to maintain schema across all Parquet files.
- **Filtering**: Use `--save-only-keyword-matches` to only save records that match at least one of the provided keywords.


The output will be saved as daily `.parquet` files in the `files/` directory.

## Project Structure
- `src/eurovoc_miner/`: Core logic and CLI.
- `files/`: Output data storage.
- `cache/`: Internal joblib cache (ignored by git).
- `tests/`: Unit and integration tests. (to do)

## Development
To add new parsers, refer to `src/eurovoc_miner/parsers.py`.
To modify the SPARQL logic, see `src/eurovoc_miner/fetcher.py`.