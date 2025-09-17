from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from curate_ns_pond.cli import app
from curate_ns_pond.storage import hash_identifiers


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


@respx.mock
def test_search_pubmed_writes_pmids(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    search_route = respx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi")

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["term"] == "neuroimaging"
        assert request.url.params["retstart"] == "0"
        payload = {
            "esearchresult": {
                "count": "2",
                "idlist": ["12345", "67890"],
            }
        }
        return httpx.Response(200, json=payload)

    search_route.mock(side_effect=_handler)

    runner = CliRunner()
    result = runner.invoke(app, ["search", "pubmed", "neuroimaging"])

    assert result.exit_code == 0, result.stdout

    search_hash = hash_identifiers(["neuroimaging"])
    run_dir = tmp_path / "raw" / "pubmed" / search_hash / "20240115"

    pmid_file = run_dir / "pmids.txt"
    metadata_file = run_dir / "metadata.json"

    assert pmid_file.exists()
    assert metadata_file.exists()

    assert pmid_file.read_text().splitlines() == ["12345", "67890"]

    metadata = json.loads(metadata_file.read_text())
    assert metadata["query"] == "neuroimaging"
    assert metadata["result_count"] == 2
    assert metadata["run_started_at"].startswith("2024-01-15")
