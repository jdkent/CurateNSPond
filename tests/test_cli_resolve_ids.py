from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from curate_ns_pond.cli import app
from curate_ns_pond.resolution import normalize_identifier
from curate_ns_pond.storage import hash_identifiers


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


@pytest.mark.vcr
def test_cli_resolve_ids_writes_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    input_path = tmp_path / "ids.txt"
    input_path.write_text("32256646\nPMC7086438\n")

    runner = CliRunner()
    result = runner.invoke(app, ["resolve", "ids", str(input_path)])

    assert result.exit_code == 0, result.stdout
    assert "Resolved 2 identifiers into 1 records" in result.stdout

    normalized = [normalize_identifier("32256646"), normalize_identifier("PMC7086438")]
    identifier_hash = hash_identifiers([item.hash_component for item in normalized])

    run_dir = tmp_path / "interim" / "resolved" / identifier_hash
    records_path = run_dir / "records.jsonl"
    metadata_path = run_dir / "metadata.json"

    assert records_path.exists()
    assert metadata_path.exists()

    records = [json.loads(line) for line in records_path.read_text().splitlines() if line]
    assert records == [
        {
            "pmid": "32256646",
            "pmcid": "PMC7086438",
            "doi": "10.1155/2020/4598217",
        }
    ]

    metadata = json.loads(metadata_path.read_text())
    assert metadata["input_count"] == 2
    assert metadata["record_count"] == 1
    assert metadata["sources"] == ["entrez", "pmc", "semantic-scholar"]
    assert metadata["input_hash"] == identifier_hash
