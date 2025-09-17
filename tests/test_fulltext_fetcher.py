from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
import vcr

from curate_ns_pond.fulltext import FullTextFetcher
from curate_ns_pond.settings import PipelineSettings


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


class DummyPubget:
    def __init__(self, responses: dict[str, str | None]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def fetch_text(self, pmcid: str) -> str | None:
        self.calls.append(pmcid)
        return self.responses.get(pmcid)


class DummyAce:
    def __init__(self, responses: dict[str, str | None]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def fetch_text(self, pmid: str) -> str | None:
        self.calls.append(pmid)
        return self.responses.get(pmid)


class DummySemantic:
    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def fetch_metadata(self, identifier: str) -> dict[str, object] | None:
        self.calls.append(identifier)
        return self.responses.get(identifier)


class DummyEntrez:
    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def fetch_metadata(self, pmid: str) -> dict[str, object] | None:
        self.calls.append(pmid)
        return self.responses.get(pmid)


CASSETTE_DIR = Path(__file__).parent / "cassettes"

vcr_recorder = vcr.VCR(
    cassette_library_dir=str(CASSETTE_DIR),
    record_mode="once",
    path_transformer=vcr.VCR.ensure_suffix(".yaml"),
    filter_headers=["user-agent", "x-api-key"],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def _write_jsonl(path: Path, records: list[dict[str, str | None]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def test_fulltext_fetcher_prefers_pubget(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(input_file, [{"pmid": "123", "pmcid": "PMC123", "doi": "10.1000/abc"}])

    fetcher = FullTextFetcher(
        settings=PipelineSettings(),
        pubget_client=DummyPubget({"PMC123": "pubget text"}),
        ace_client=DummyAce({}),
        semantic_client=DummySemantic(
            {
                "123": {
                    "title": "Sample Title",
                    "abstract": "Sample Abstract",
                    "authors": ["Ada Lovelace"],
                    "journal": "Journal",
                    "year": 2023,
                }
            }
        ),
        entrez_client=DummyEntrez({}),
    )

    result = fetcher.fetch_from_files([input_file])

    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.sources_used == {"pubget"}
    assert result.metadata_sources_used == {"semantic-scholar"}
    assert not result.errors

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    document_files = sorted((run_dir / "records").glob("*.json"))
    assert len(document_files) == 1
    payload = json.loads(document_files[0].read_text())
    assert payload["text_source"] == "pubget"
    assert payload["text"] == "pubget text"
    assert payload["metadata"]["title"] == "Sample Title"

    metadata = json.loads((run_dir / "metadata.json").read_text())
    assert metadata["record_count"] == 1
    assert metadata["records_with_text"] == 1
    assert metadata["records_without_text"] == 0


def test_fulltext_fetcher_falls_back_to_ace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(input_file, [{"pmid": "456", "pmcid": "PMC456", "doi": None}])

    fetcher = FullTextFetcher(
        settings=PipelineSettings(),
        pubget_client=DummyPubget({"PMC456": None}),
        ace_client=DummyAce({"456": "ace text"}),
        semantic_client=DummySemantic({}),
        entrez_client=DummyEntrez(
            {
                "456": {
                    "title": "Entrez Title",
                    "abstract": "Entrez Abstract",
                    "authors": ["Grace Hopper"],
                    "journal": "Entrez Journal",
                    "year": 2020,
                }
            }
        ),
    )

    result = fetcher.fetch_from_files([input_file])

    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.sources_used == {"ace"}
    assert result.metadata_sources_used == {"entrez"}
    assert any("PMC456" in message for message in result.errors)

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["text_source"] == "ace"
    assert payload["metadata"]["title"] == "Entrez Title"


def test_fulltext_fetcher_records_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(input_file, [{"pmid": "789", "pmcid": None, "doi": None}])

    fetcher = FullTextFetcher(
        settings=PipelineSettings(),
        pubget_client=DummyPubget({}),
        ace_client=DummyAce({"789": None}),
        semantic_client=DummySemantic({}),
        entrez_client=DummyEntrez({}),
    )

    result = fetcher.fetch_from_files([input_file])

    assert result.success_count == 0
    assert result.failure_count == 1
    assert "789" in " ".join(result.errors)

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["text"] is None
    assert payload["metadata"] is None


def test_fulltext_fetcher_with_semantic_metadata(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(
        input_file,
        [
            {"pmid": "1", "pmcid": "PMC1", "doi": "10.1/example"},
        ],
    )

    fetcher = FullTextFetcher(
        settings=PipelineSettings(),
        pubget_client=DummyPubget({"PMC1": "pubget text"}),
        ace_client=DummyAce({}),
    )

    try:
        with vcr_recorder.use_cassette("fulltext_semantic"):
            result = fetcher.fetch_from_files([input_file])
    finally:
        fetcher._semantic.close()
        fetcher._entrez.close()

    assert result.success_count == 1
    assert result.sources_used == {"pubget"}
    assert result.metadata_sources_used == {"semantic-scholar"}

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["metadata"]["title"] == "VCR Sample Title"
    assert payload["metadata"]["authors"] == ["Author One"]


def test_fulltext_fetcher_falls_back_to_entrez_with_vcr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(
        input_file,
        [
            {"pmid": "2", "pmcid": "PMC2", "doi": None},
        ],
    )

    fetcher = FullTextFetcher(
        settings=PipelineSettings(),
        pubget_client=DummyPubget({"PMC2": None}),
        ace_client=DummyAce({"2": "ace text"}),
    )

    try:
        with vcr_recorder.use_cassette("fulltext_entrez"):
            result = fetcher.fetch_from_files([input_file])
    finally:
        fetcher._semantic.close()
        fetcher._entrez.close()

    assert result.success_count == 1
    assert "PubGet returned no text for PMC2" in result.errors[0]
    assert result.sources_used == {"ace"}
    assert result.metadata_sources_used == {"entrez"}

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["text_source"] == "ace"
    assert payload["metadata"]["title"] == "Entrez Example Title"
