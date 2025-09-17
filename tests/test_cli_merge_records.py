from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from curate_ns_pond.cli import app
from curate_ns_pond.merge import merge_jsonl_files


class _FixedDatetime(datetime):
    @classmethod
    def utcnow(cls) -> "_FixedDatetime":
        return cls(2024, 1, 15, 12, 0, 0)


def _write_jsonl(path: Path, records: list[dict[str, str | None]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def test_cli_merge_records(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("curate_ns_pond.cli.datetime", _FixedDatetime)

    file_a = tmp_path / "a.jsonl"
    file_b = tmp_path / "b.jsonl"

    _write_jsonl(file_a, [{"pmid": "1"}])
    _write_jsonl(file_b, [{"pmid": "1", "pmcid": "PMC1", "doi": "10.1/ABC"}])

    runner = CliRunner()
    result = runner.invoke(app, ["merge", "records", str(file_a), str(file_b)])

    assert result.exit_code == 0, result.stdout
    assert "Merged 1 records" in result.stdout

    outcome = merge_jsonl_files([file_a, file_b])

    run_dir = tmp_path / "processed" / "merged" / outcome.input_hash
    records_path = run_dir / "records.jsonl"
    metadata_path = run_dir / "metadata.json"

    assert records_path.exists()
    assert metadata_path.exists()

    records = [json.loads(line) for line in records_path.read_text().splitlines() if line]
    assert records == [{"pmid": "1", "pmcid": "PMC1", "doi": "10.1/abc"}]

    metadata = json.loads(metadata_path.read_text())
    assert metadata["input_files"] == [str(file_a), str(file_b)]
    assert metadata["input_hash"] == outcome.input_hash
    assert metadata["record_count"] == 1
    assert metadata["run_started_at"].startswith("2024-01-15")
