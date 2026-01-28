import os
import logging

# Centralized configuration for the Eurovoc Miner

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(BASE_DIR)
FILES_DIR = os.path.join(ROOT_DIR, 'files')
CACHE_DIR = os.path.join(ROOT_DIR, 'cache')
TEMPLATES_DIR = os.path.join(BASE_DIR, 'eurovoc_miner', 'templates')

# Create necessary directories
os.makedirs(FILES_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Logging
LOG_FILE = os.path.join(ROOT_DIR, 'collect.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Network
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
EUROVOC_XML_URL = 'http://publications.europa.eu/resource/dataset/eurovoc'

# Concurrency
MAX_WORKERS = os.cpu_count() or 1

# Data Schema
import polars as pl
SCHEMA = {
    "url": pl.String,
    "celex": pl.String,
    "eli": pl.String,
    "title": pl.String,
    "date": pl.String,
    "lang": pl.String,
    "institutions": pl.List(pl.String),
    "work_types": pl.List(pl.String),
    "procedure_ids": pl.List(pl.String),
    "directory_codes": pl.List(pl.String),
    "formats": pl.List(pl.String),
    "eurovoc_concepts": pl.List(pl.String),
    "eurovoc_concepts_ids": pl.List(pl.String),
    "text": pl.String,
}
