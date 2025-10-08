from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from curate_ns_pond.fulltext import ACEClient, FullTextFetcher, PubGetClient
from curate_ns_pond.settings import PipelineSettings


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


def _write_jsonl(path: Path, records: list[dict[str, str | None]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def _skip_if_ace_missing() -> None:
    try:
        __import__("ace.scrape")
    except ModuleNotFoundError:
        pytest.skip("ACE package not available; install the 'fulltext' extra to run this test")


@pytest.mark.vcr
def test_fulltext_fetcher_prefers_pubget(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    # Ensure the pubget CLI returns deterministic content without invoking the network.
    monkeypatch.setattr(
        PubGetClient,
        "fetch_text",
        lambda self, pmcid: "pubget text" if pmcid == "PMC7086438" else None,
    )

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(
        input_file,
        [
            {
                "pmid": "32256646",
                "pmcid": "PMC7086438",
                "doi": "10.1155/2020/4598217",
            }
        ],
    )

    fetcher = FullTextFetcher(settings=PipelineSettings())

    try:
        result = fetcher.fetch_from_files([input_file])
    finally:
        fetcher._semantic.close()
        fetcher._entrez.close()

    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.sources_used == {"pubget"}
    assert "entrez" in result.metadata_sources_used

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["text_source"] == "pubget"
    assert payload["text"] == "pubget text"
    assert "Nano Leo" in (payload["metadata"]["title"] or "")
    assert payload["metadata"]["source"] in {"semantic-scholar", "entrez"}


@pytest.mark.vcr
def test_fulltext_fetcher_falls_back_to_ace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _skip_if_ace_missing()

    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    # Force PubGet failure so the fetcher uses ACE as a fallback path.
    monkeypatch.setattr(PubGetClient, "fetch_text", lambda self, pmcid: None)
    monkeypatch.setattr(ACEClient, "fetch_text", lambda self, pmid: "ace text" if pmid == "31452104" else None)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(
        input_file,
        [
            {
                "pmid": "31452104",
                "pmcid": None,
                "doi": "10.1007/978-1-4939-9752-7_10",
            }
        ],
    )

    fetcher = FullTextFetcher(settings=PipelineSettings())

    try:
        result = fetcher.fetch_from_files([input_file])
    finally:
        fetcher._semantic.close()
        fetcher._entrez.close()

    assert result.success_count == 1
    assert result.sources_used == {"ace"}
    assert "semantic-scholar" in result.metadata_sources_used

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["text_source"] == "ace"
    assert payload["text"] == "ace text"
    assert "Molegro Virtual Docker" in (payload["metadata"]["title"] or "")
    assert payload["metadata"]["source"] in {"semantic-scholar", "entrez"}


@pytest.mark.vcr
def test_fulltext_fetcher_records_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.fulltext.datetime", _FixedDatetime)

    # Simulate missing full text across all providers.
    monkeypatch.setattr(PubGetClient, "fetch_text", lambda self, pmcid: None)
    monkeypatch.setattr(ACEClient, "fetch_text", lambda self, pmid: None)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(input_file, [{"pmid": "999999999", "pmcid": None, "doi": None}])

    fetcher = FullTextFetcher(settings=PipelineSettings())

    try:
        result = fetcher.fetch_from_files([input_file])
    finally:
        fetcher._semantic.close()
        fetcher._entrez.close()

    assert result.success_count == 0
    assert result.failure_count == 1
    assert "999999999" in " ".join(result.errors)
    assert not result.metadata_sources_used

    run_dir = tmp_path / "processed" / "fulltext" / result.batch_hash
    payload = json.loads(next((run_dir / "records").glob("*.json")).read_text())
    assert payload["text"] is None
    assert payload["metadata"] is None
