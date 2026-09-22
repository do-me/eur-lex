# Experimental Roaring search index

This is an optional add-on. The existing `weekly_sync.yml`, miner code, and
Parquet upload path are unchanged. The separate `roaring_index.yml` workflow
runs after a successful weekly sync or by manual dispatch. It uses the existing
`HF_TOKEN` secret but writes **only** to the dataset repo's `search-index`
branch, under `search/roaring/v1/`. The dataset's `main` branch remains the
source of truth for Parquet.

To initialize or rebuild the archive, manually dispatch **Experimental Roaring
Search Index** with `mode=bootstrap`. Bootstrap may download and index the
entire archive and should be watched separately from the normal 30-minute
mining job. Weekly `mode=update` downloads and rebuilds only the current year
and, around New Year, the preceding year if their Parquet content fingerprint
changed. Older-year corrections require another manual bootstrap. No index
files are deleted automatically.

The publisher uploads each complete shard to an immutable path containing the
source revision and pinned builder revision. It publishes the root manifest
*last*. Each shard points phrase verification to the matching, immutable
Parquet revision. The [static search page](https://do-me.github.io/roaring-static-search/)
is hosted by the separate Roaring repository and reads the index through
Hugging Face HTTP Range requests; it does not run a search service.

To pause all automatic and manual index jobs, set repository variable
`ROARING_INDEX_ENABLED=false` in `do-me/eur-lex`. The existing weekly miner
continues unaffected. To fully revert the experiment, disable/delete the new
workflow and delete only the `search-index` branch on Hugging Face; no source
Parquet or dataset `main` history needs reverting. Re-enable by removing the
variable or setting it to `true`, then dispatch `bootstrap` if the branch was
deleted.
