from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from curate_ns_pond.cli import app
from curate_ns_pond.storage import hash_identifiers


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


@pytest.mark.vcr
def test_search_pubmed_writes_pmids(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "search",
            "pubmed",
            "31452104[pmid] OR 31722068[pmid]",
            "--retmax",
            "1",
        ],
    )

    assert result.exit_code == 0, result.stdout

    search_hash = hash_identifiers(["31452104[pmid] OR 31722068[pmid]"])
    run_dir = tmp_path / "raw" / "pubmed" / search_hash / "20240115"

    pmid_file = run_dir / "pmids.txt"
    metadata_file = run_dir / "metadata.json"

    assert pmid_file.exists()
    assert metadata_file.exists()

    pmids = pmid_file.read_text().splitlines()
    assert set(pmids) == {"31452104", "31722068"}

    metadata = json.loads(metadata_file.read_text())
    assert metadata["query"] == "31452104[pmid] OR 31722068[pmid]"
    assert metadata["result_count"] == 2
    assert metadata["run_started_at"].startswith("2024-01-15")
