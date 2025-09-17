from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from curate_ns_pond.cli import app


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


def _write_jsonl(path: Path, records: list[dict[str, str | None]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


class StubPubget:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_text(self, pmcid: str) -> str | None:
        self.calls.append(pmcid)
        return "stub pubget text" if pmcid == "PMC1" else None


class StubAce:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_text(self, pmid: str) -> str | None:
        self.calls.append(pmid)
        return "stub ace text"


class StubSemantic:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_metadata(self, identifier: str) -> dict[str, object] | None:
        self.calls.append(identifier)
        if identifier == "1":
            return {
                "title": "Stub Title",
                "abstract": "Stub Abstract",
                "authors": ["Test Author"],
                "journal": "Stub Journal",
                "year": 2024,
            }
        return None


class StubEntrez:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_metadata(self, pmid: str) -> dict[str, object] | None:
        self.calls.append(pmid)
        return None


def test_cli_fetch_fulltext(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    monkeypatch.setattr("curate_ns_pond.fulltext.PubGetClient", StubPubget)
    monkeypatch.setattr("curate_ns_pond.fulltext.ACEClient", StubAce)
    monkeypatch.setattr("curate_ns_pond.fulltext.SemanticScholarClient", StubSemantic)
    monkeypatch.setattr("curate_ns_pond.fulltext.EntrezSummaryClient", StubEntrez)

    input_file = tmp_path / "records.jsonl"
    _write_jsonl(input_file, [{"pmid": "1", "pmcid": "PMC1", "doi": "10.1/abc"}])

    runner = CliRunner()
    result = runner.invoke(app, ["fetch", "fulltext", str(input_file)])

    assert result.exit_code == 0, result.stdout
    assert "Fetched full text for 1 of 1 records" in result.stdout

    records_dir = tmp_path / "processed" / "fulltext"
    json_files = list(records_dir.rglob("*.json"))
    assert any(file.name.endswith(".json") for file in json_files)
