"""Resume a compact-data Hub PR in bounded, independently verified year batches.

Manual migration only. The source revision is immutable and public main is
checked before every upload. Only refs/pr/N is written; main is never changed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download
from huggingface_hub.hf_api import RepoFile

from .compact_hf_snapshot import rebuild

DATASET = "do-me/EUR-LEX"


def verify_years(api: HfApi, revision: str, output: Path, years: list[int]) -> int:
    verified = 0
    for year in years:
        local = {f"files/{year}/{path.name}": path
                 for path in (output / "files" / str(year)).glob("*.parquet")}
        remote = {entry.path: entry for entry in api.list_repo_tree(
            DATASET, repo_type="dataset", revision=revision,
            path_in_repo=f"files/{year}", recursive=True, expand=True,
        ) if isinstance(entry, RepoFile)}
        if local.keys() != remote.keys():
            raise ValueError(f"Staged file inventory differs for {year}")
        for name, path in local.items():
            entry = remote[name]
            if entry.lfs is None or path.stat().st_size != entry.size:
                raise ValueError(f"Staged file metadata differs: {name}")
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(4 << 20), b""):
                    digest.update(block)
            if digest.hexdigest() != entry.lfs.sha256:
                raise ValueError(f"Staged file hash differs: {name}")
            verified += 1
    return verified


def resume(source_revision: str, pr_number: int) -> None:
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN is required")
    if len(source_revision) != 40 or pr_number <= 0:
        raise ValueError("Expected a full source commit and positive PR number")
    pr_revision = f"refs/pr/{pr_number}"
    api = HfApi(token=token)
    folders = list(api.list_repo_tree(DATASET, repo_type="dataset",
                                      revision=source_revision, path_in_repo="files"))
    years = sorted(int(entry.path.split("/")[1]) for entry in folders
                   if entry.path.startswith("files/") and entry.path.split("/")[1].isdigit())
    if not years:
        raise ValueError("No source years were found")
    print(json.dumps({"event": "plan", "years": years, "pr": pr_number,
                      "sourceRevision": source_revision}), flush=True)

    for index in range(0, len(years), 5):
        band = years[index:index + 5]
        main_revision = api.repo_info(DATASET, repo_type="dataset", revision="main").sha
        if main_revision != source_revision:
            raise RuntimeError(f"Public main changed before {band}: {main_revision}")
        with tempfile.TemporaryDirectory(prefix="eurlex-compact-band-") as scratch:
            root = Path(scratch)
            source = root / "source"
            output = root / "candidate"
            patterns = ["README.md", ".gitattributes"] + [f"files/{year}/*.parquet" for year in band]
            snapshot_download(DATASET, repo_type="dataset", revision=source_revision,
                              local_dir=source, allow_patterns=patterns, token=token, max_workers=16)
            report = rebuild(source, output)
            if report["parquetFiles"] == 0:
                raise ValueError(f"Downloaded no source files for {band}")
            parent = api.repo_info(DATASET, repo_type="dataset", revision=pr_revision).sha
            api.upload_folder(
                repo_id=DATASET, repo_type="dataset", revision=pr_revision,
                parent_commit=parent, folder_path=str(output / "files"),
                path_in_repo="files", token=token,
                commit_message=f"Compact EUR-LEX Parquet {band[0]}-{band[-1]}",
            )
            staged = api.repo_info(DATASET, repo_type="dataset", revision=pr_revision).sha
            verified = verify_years(api, staged, output, band)
            if verified != report["parquetFiles"]:
                raise ValueError(f"Incomplete staged verification for {band}")
            print(json.dumps({"event": "band_verified", "years": band,
                              "files": verified, "oldBytes": report["oldBytes"],
                              "newBytes": report["newBytes"], "stagedRevision": staged}), flush=True)

    print(json.dumps({"event": "all_years_verified", "years": len(years),
                      "stagedRevision": api.repo_info(DATASET, repo_type="dataset",
                                                      revision=pr_revision).sha}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--pr", type=int, required=True)
    args = parser.parse_args()
    resume(args.source_revision, args.pr)


if __name__ == "__main__":
    main()
