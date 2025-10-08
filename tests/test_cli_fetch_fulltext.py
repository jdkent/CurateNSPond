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


@pytest.mark.vcr
def test_cli_fetch_fulltext(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    # PubGet requires an external CLI; return deterministic text to keep the test hermetic.
    monkeypatch.setattr(
        "curate_ns_pond.fulltext.PubGetClient.fetch_text",
        lambda self, pmcid: "pubget text" if pmcid == "PMC7086438" else None,
    )

    # ACE should not be needed when PubGet succeeds, but guard against network calls.
    monkeypatch.setattr(
        "curate_ns_pond.fulltext.ACEClient.fetch_text",
        lambda self, pmid: None,
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

    runner = CliRunner()
    result = runner.invoke(app, ["fetch", "fulltext", str(input_file)])

    assert result.exit_code == 0, result.stdout
    assert "Fetched full text for 1 of 1 records" in result.stdout

    records_dir = tmp_path / "processed" / "fulltext"
    json_files = list(records_dir.rglob("*.json"))
    assert any(file.name.endswith(".json") for file in json_files)
