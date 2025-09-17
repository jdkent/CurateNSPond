from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from curate_ns_pond.cli import app
from curate_ns_pond.resolution import normalize_identifier
from curate_ns_pond.storage import hash_identifiers


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


@respx.mock
def test_cli_resolve_ids_writes_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    input_path = tmp_path / "ids.txt"
    input_path.write_text("123456\nPMC123456\n")

    respx.get("https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/").mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"pmcid": "PMC123456", "pmid": "123456", "doi": "10.1000/xyz"}
                ]
            },
        )
    )
    respx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": {
                    "uids": ["123456"],
                    "123456": {
                        "articleids": [
                            {"idtype": "pmid", "value": "123456"},
                            {"idtype": "pmcid", "value": "PMC123456"},
                            {"idtype": "doi", "value": "10.1000/xyz"},
                        ]
                    },
                }
            },
        )
    )
    respx.get("https://api.semanticscholar.org/graph/v1/paper/123456").mock(
        return_value=httpx.Response(
            200,
            json={
                "externalIds": {
                    "PMID": "123456",
                    "PMCID": "PMC123456",
                    "DOI": "10.1000/xyz",
                }
            },
        )
    )
    respx.get("https://api.semanticscholar.org/graph/v1/paper/10.1000%2Fxyz").mock(
        return_value=httpx.Response(
            200,
            json={
                "externalIds": {
                    "PMID": "123456",
                    "PMCID": "PMC123456",
                    "DOI": "10.1000/xyz",
                }
            },
        )
    )

    runner = CliRunner()
    result = runner.invoke(app, ["resolve", "ids", str(input_path)])

    assert result.exit_code == 0, result.stdout
    assert "Resolved 2 identifiers into 1 records" in result.stdout

    normalized = [normalize_identifier("123456"), normalize_identifier("PMC123456")]
    identifier_hash = hash_identifiers([item.hash_component for item in normalized])

    run_dir = tmp_path / "interim" / "resolved" / identifier_hash
    records_path = run_dir / "records.jsonl"
    metadata_path = run_dir / "metadata.json"

    assert records_path.exists()
    assert metadata_path.exists()

    records = [json.loads(line) for line in records_path.read_text().splitlines() if line]
    assert records == [
        {"pmid": "123456", "pmcid": "PMC123456", "doi": "10.1000/xyz"}
    ]

    metadata = json.loads(metadata_path.read_text())
    assert metadata["input_count"] == 2
    assert metadata["record_count"] == 1
    assert metadata["sources"] == ["entrez", "pmc", "semantic-scholar"]
    assert metadata["input_hash"] == identifier_hash
