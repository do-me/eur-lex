"""Experimental, independently disableable Roaring index publisher.

Only the Hugging Face ``search-index`` branch is written. Dataset ``main`` and
the existing mining/upload workflow are never modified by this module.
The root manifest is uploaded last, after every immutable shard is present.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download, snapshot_download
from huggingface_hub.errors import EntryNotFoundError, RevisionNotFoundError
from huggingface_hub.hf_api import RepoFile
from roaring_static_search.builder import build_parquet_source_map, build_shard, parquet_records
from roaring_static_search.format import FORMAT


DATASET = "do-me/EUR-LEX"
INDEX_BRANCH = "search-index"
INDEX_ROOT = "search/roaring/v1"
COLUMNS = ["celex", "text", "title", "date", "url"]


def source_files(
    api: HfApi, revision: str, *, year: int | None = None, expand: bool = True,
) -> list[RepoFile]:
    folder = f"files/{year}" if year is not None else "files"
    try:
        entries = api.list_repo_tree(
            DATASET, repo_type="dataset", revision=revision,
            path_in_repo=folder, recursive=True, expand=expand,
        )
        return sorted(
            (entry for entry in entries if isinstance(entry, RepoFile)
             and entry.path.endswith(".parquet") and entry.path.startswith(folder + "/")),
            key=lambda entry: entry.path,
        )
    except EntryNotFoundError:
        return []


def archive_source_files(api: HfApi, revision: str, *, before_year: int) -> list[RepoFile]:
    # Only the changing years need expanded LFS metadata for fingerprinting.
    # The default tree endpoint paginates far more efficiently on large repos.
    return [entry for entry in source_files(api, revision, expand=False)
            if entry.path.split("/")[1].isdigit()
            and int(entry.path.split("/")[1]) < before_year]


def file_fingerprint(files: list[RepoFile]) -> str:
    digest = hashlib.sha256()
    for entry in files:
        lfs = entry.lfs or {}
        identity = (lfs.get("sha256") or lfs.get("oid")
                    or getattr(entry, "xet_hash", None) or entry.blob_id)
        if not identity:
            raise ValueError(f"No content identity for {entry.path}")
        digest.update(f"{entry.path}\0{entry.size}\0{identity}\n".encode())
    return digest.hexdigest()


def existing_manifest(token: str) -> dict | None:
    try:
        path = hf_hub_download(
            DATASET, f"{INDEX_ROOT}/manifest.json", repo_type="dataset",
            revision=INDEX_BRANCH, token=token,
        )
    except (EntryNotFoundError, RevisionNotFoundError):
        return None
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    if manifest.get("format") != FORMAT:
        raise ValueError("Existing search manifest has an incompatible format")
    return manifest


def build_and_upload(
    api: HfApi, token: str, source_revision: str, builder_revision: str,
    files: list[RepoFile], name: str,
) -> str:
    if not files:
        raise ValueError(f"No source Parquet files for {name}")
    relative_dir = f"revisions/{source_revision}/{builder_revision[:12]}/{name}"
    destination = f"{INDEX_ROOT}/{relative_dir}"
    with tempfile.TemporaryDirectory(prefix=f"roaring-{name}-") as scratch:
        root = Path(scratch) / "source"
        root.mkdir()
        if name == "archive":
            patterns = [f"files/{year}/*.parquet" for year in sorted(
                {entry.path.split("/")[1] for entry in files})]
        else:
            patterns = [f"files/{name.removeprefix('year')}/*.parquet"]
        snapshot_download(
            DATASET, repo_type="dataset", revision=source_revision,
            local_dir=root, allow_patterns=patterns, token=token, max_workers=8,
        )
        paths = [root / entry.path for entry in files]
        for entry, path in zip(files, paths, strict=True):
            if not path.is_file() or path.stat().st_size != entry.size:
                raise ValueError(f"Missing or mismatched source file: {entry.path}")
        output = Path(scratch) / "shard"
        result = build_shard(
            parquet_records(paths, COLUMNS), output,
            id_field="celex", text_field="text",
            metadata_fields=["title", "date", "url"], store_text=False,
        )
        locator = build_parquet_source_map(paths, root, output / "sources.json.gz", result["documentCount"])
        result["externalText"] = {
            "map": "sources.json.gz",
            "baseUrl": f"https://huggingface.co/datasets/{DATASET}/resolve/{source_revision}/",
            "mode": "whole",
            "concurrency": 8,
        }
        result["files"]["sources.json.gz"] = locator["gzipBytes"]
        (output / "shard.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"event": "built", "name": name, "sourceFiles": len(files),
                          "documents": result["documentCount"], "destination": destination}), flush=True)
        api.upload_folder(
            repo_id=DATASET, repo_type="dataset", revision=INDEX_BRANCH,
            folder_path=str(output), path_in_repo=destination, token=token,
            commit_message=f"Roaring index {name} from {source_revision[:12]}",
        )
    return f"{relative_dir}/shard.json"


def publish(mode: str, builder_revision: str, *, now: datetime | None = None) -> dict:
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN is required for the separate index publisher")
    api = HfApi(token=token)
    source_revision = api.repo_info(DATASET, repo_type="dataset", revision="main").sha
    if not source_revision:
        raise ValueError("Could not determine immutable dataset source revision")
    today = (now or datetime.now(timezone.utc)).date()
    year = today.year
    affected = sorted({year, (today - timedelta(days=60)).year})
    if mode == "bootstrap":
        api.create_branch(DATASET, branch=INDEX_BRANCH, revision=source_revision,
                          repo_type="dataset", token=token, exist_ok=True)
        old = None
    else:
        old = existing_manifest(token)
        if old is None:
            raise ValueError("No index manifest yet; dispatch the bootstrap mode first")

    shards = [] if old is None else list(old["shards"])
    fingerprints = {} if old is None else dict(old.get("yearFingerprints", {}))
    if mode == "bootstrap":
        archive_files = archive_source_files(api, source_revision, before_year=min(affected))
        archive_url = build_and_upload(api, token, source_revision, builder_revision,
                                       archive_files, "archive")
        shards.append({"name": "archive", "url": archive_url})

    changed = mode == "bootstrap"
    for affected_year in affected:
        year_files = source_files(api, source_revision, year=affected_year)
        if not year_files:
            continue
        fingerprint = file_fingerprint(year_files)
        if mode != "bootstrap" and fingerprints.get(str(affected_year)) == fingerprint:
            print(json.dumps({"event": "unchanged", "year": affected_year}), flush=True)
            continue
        name = f"year{affected_year}"
        shard_url = build_and_upload(api, token, source_revision, builder_revision,
                                     year_files, name)
        shards = [item for item in shards if item["name"] != name]
        shards.append({"name": name, "url": shard_url})
        fingerprints[str(affected_year)] = fingerprint
        changed = True

    if not changed:
        print(json.dumps({"event": "no-op", "sourceRevision": source_revision}), flush=True)
        return old
    shards.sort(key=lambda item: -1 if item["name"] == "archive" else int(item["name"].removeprefix("year")))
    manifest = {
        "format": FORMAT,
        "sourceRevision": source_revision,
        "builderRevision": builder_revision,
        "yearFingerprints": fingerprints,
        "shards": shards,
    }
    with tempfile.TemporaryDirectory(prefix="roaring-manifest-") as scratch:
        path = Path(scratch) / "manifest.json"
        path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        api.upload_file(
            repo_id=DATASET, repo_type="dataset", revision=INDEX_BRANCH,
            path_or_fileobj=str(path), path_in_repo=f"{INDEX_ROOT}/manifest.json",
            token=token, commit_message=f"Publish Roaring search manifest from {source_revision[:12]}",
        )
    print(json.dumps({"event": "published", "shards": len(shards),
                      "sourceRevision": source_revision}), flush=True)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("bootstrap", "update"), required=True)
    parser.add_argument("--builder-revision", required=True)
    args = parser.parse_args()
    publish(args.mode, args.builder_revision)


if __name__ == "__main__":
    main()
