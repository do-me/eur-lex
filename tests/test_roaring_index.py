"""No-network tests for the isolated index publish plan."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import roaring_index


class FakeApi:
    def __init__(self, token):
        self.calls = []

    def repo_info(self, *args, **kwargs):
        return SimpleNamespace(sha="a" * 40)

    def create_branch(self, *args, **kwargs):
        self.calls.append(("branch", kwargs["branch"]))

    def upload_file(self, **kwargs):
        self.calls.append(("manifest", json.loads(open(kwargs["path_or_fileobj"], encoding="utf-8").read())))


def source(path, identity):
    return SimpleNamespace(path=path, size=100, lfs={"sha256": identity}, blob_id=None)


def test_archive_listing_uses_fast_unexpanded_tree(monkeypatch):
    calls = []
    entries = [source("files/2025/a.parquet", "one"),
               source("files/2026/b.parquet", "two")]

    def fake_source_files(api, revision, *, year=None, expand=True):
        calls.append((revision, year, expand))
        return entries

    monkeypatch.setattr(roaring_index, "source_files", fake_source_files)
    result = roaring_index.archive_source_files(object(), "revision", before_year=2026)
    assert [item.path for item in result] == ["files/2025/a.parquet"]
    assert calls == [("revision", None, False)]


def test_history_is_split_into_five_year_bands_from_cutoff():
    files = [source(f"files/{year}/a.parquet", str(year)) for year in range(2014, 2026)]
    bands = roaring_index.historical_bands(files, before_year=2026)
    assert [(name, start, end) for name, start, end, _ in bands] == [
        ("years2011-2015", 2014, 2015),
        ("years2016-2020", 2016, 2020),
        ("years2021-2025", 2021, 2025),
    ]


def test_bootstrap_keeps_affected_previous_year_out_of_archive(monkeypatch):
    api = FakeApi("test")
    files = [source("files/2025/a.parquet", "one"), source("files/2026/b.parquet", "two"),
             source("files/2027/c.parquet", "three")]
    built = []
    monkeypatch.setenv("HF_TOKEN", "test")
    monkeypatch.setattr(roaring_index, "HfApi", lambda token: api)
    monkeypatch.setattr(roaring_index, "archive_source_files", lambda api, revision, before_year:
                        [item for item in files if int(item.path.split("/")[1]) < before_year])
    monkeypatch.setattr(roaring_index, "source_files", lambda api, revision, year=None, expand=True:
                        files if year is None else [item for item in files if f"files/{year}/" in item.path])
    def fake_build(api, token, source_revision, builder_revision, selected, name):
        built.append((name, [item.path for item in selected]))
        return f"revisions/{name}/shard.json"
    monkeypatch.setattr(roaring_index, "build_and_upload", fake_build)
    result = roaring_index.publish("bootstrap", "builder", now=datetime(2027, 1, 10, tzinfo=timezone.utc))
    assert built == [
        ("years2021-2025", ["files/2025/a.parquet"]),
        ("year2026", ["files/2026/b.parquet"]),
        ("year2027", ["files/2027/c.parquet"]),
    ]
    assert result["shards"] == [
        {"name": "years2021-2025", "url": "revisions/years2021-2025/shard.json", "yearStart": 2025, "yearEnd": 2025},
        {"name": "year2026", "url": "revisions/year2026/shard.json", "yearStart": 2026, "yearEnd": 2026},
        {"name": "year2027", "url": "revisions/year2027/shard.json", "yearStart": 2027, "yearEnd": 2027},
    ]
    assert result["layoutVersion"] == 2
    assert api.calls[0] == ("branch", "search-index")
    assert api.calls[-1][0] == "manifest"


def test_unchanged_weekly_run_is_noop(monkeypatch):
    api = FakeApi("test")
    files = [source("files/2026/a.parquet", "same")]
    old = {"format": roaring_index.FORMAT, "shards": [{"name": "archive", "url": "older/shard.json"},
           {"name": "year2026", "url": "current/shard.json"}],
           "yearFingerprints": {"2026": roaring_index.file_fingerprint(files)}}
    monkeypatch.setenv("HF_TOKEN", "test")
    monkeypatch.setattr(roaring_index, "HfApi", lambda token: api)
    monkeypatch.setattr(roaring_index, "existing_manifest", lambda token: old)
    monkeypatch.setattr(roaring_index, "source_files", lambda api, revision, year=None, expand=True: files)
    monkeypatch.setattr(roaring_index, "build_and_upload", lambda *args: (_ for _ in ()).throw(AssertionError("rebuilt")))
    result = roaring_index.publish("update", "builder", now=datetime(2026, 9, 22, tzinfo=timezone.utc))
    assert result is old
    assert api.calls == []


def test_changed_year_replaces_only_its_shard(monkeypatch):
    api = FakeApi("test")
    files = [source("files/2026/a.parquet", "changed")]
    old = {"format": roaring_index.FORMAT, "shards": [{"name": "archive", "url": "older/shard.json"},
           {"name": "year2026", "url": "previous/shard.json"}],
           "yearFingerprints": {"2026": "different"}}
    monkeypatch.setenv("HF_TOKEN", "test")
    monkeypatch.setattr(roaring_index, "HfApi", lambda token: api)
    monkeypatch.setattr(roaring_index, "existing_manifest", lambda token: old)
    monkeypatch.setattr(roaring_index, "source_files", lambda api, revision, year=None, expand=True: files)
    monkeypatch.setattr(roaring_index, "build_and_upload", lambda *args: "new/shard.json")
    result = roaring_index.publish("update", "builder", now=datetime(2026, 9, 22, tzinfo=timezone.utc))
    assert result["shards"] == [{"name": "archive", "url": "older/shard.json"},
                                 {"name": "year2026", "url": "new/shard.json",
                                  "yearStart": 2026, "yearEnd": 2026}]
    assert api.calls[-1][0] == "manifest"


def test_real_shard_build_is_textless_and_uploads_to_index_branch(monkeypatch, tmp_path):
    source_path = tmp_path / "fixture/files/2026/sample.parquet"
    source_path.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "celex": ["A", "B"], "text": ["greenhouse gas", "greenhouse and gas"],
        "title": ["A title", "B title"], "date": ["2026-01-01", "2026-01-02"],
        "url": ["https://example.test/a", "https://example.test/b"],
    }), source_path, compression="zstd")
    entry = SimpleNamespace(path="files/2026/sample.parquet", size=source_path.stat().st_size)

    def fake_snapshot(*args, local_dir, **kwargs):
        destination = Path(local_dir) / entry.path
        destination.parent.mkdir(parents=True)
        shutil.copyfile(source_path, destination)

    class UploadApi:
        uploaded = None

        def upload_folder(self, **kwargs):
            folder = Path(kwargs["folder_path"])
            manifest = json.loads((folder / "shard.json").read_text())
            assert (folder / "text.bin").stat().st_size == 0
            assert (folder / "sources.json.gz").stat().st_size > 0
            assert manifest["documentCount"] == 2
            assert manifest["externalText"]["baseUrl"].endswith("/" + "a" * 40 + "/")
            self.uploaded = kwargs

    monkeypatch.setattr(roaring_index, "snapshot_download", fake_snapshot)
    api = UploadApi()
    relative = roaring_index.build_and_upload(api, "test", "a" * 40, "b" * 40, [entry], "year2026")
    assert relative.endswith("/year2026/shard.json")
    assert api.uploaded["revision"] == "search-index"
    assert api.uploaded["path_in_repo"].startswith("search/roaring/v1/revisions/")
