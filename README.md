# Eurovoc Miner 🇪🇺

A high-performance mining tool for extracting text and semantic concepts from the European Commission's Cellar database.

## Features
- **Fast SPARQL Retrieval**: Custom J2-templated queries for efficient metadata fetching.
- **Parallel Parsing**: Multi-threaded extraction from PDF, DOCX, and HTML.
- **Vectorized Preprocessing**: Ultra-fast text cleaning powered by `Polars`.
- **Modular Design**: Clean separation of concerns following modern Python package standards.
- **Robust Caching**: Joblib-powered caching to avoid redundant downloads and expensive parsing.

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
```

The output will be saved as daily `.parquet` files in the `files/` directory.

## Project Structure
- `src/eurovoc_miner/`: Core logic and CLI.
- `files/`: Output data storage.
- `cache/`: Internal joblib cache (ignored by git).
- `tests/`: Unit and integration tests.

## Development
To add new parsers, refer to `src/eurovoc_miner/parsers.py`.
To modify the SPARQL logic, see `src/eurovoc_miner/fetcher.py`.