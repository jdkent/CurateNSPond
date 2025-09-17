from __future__ import annotations

import json
from pathlib import Path

import pytest

from curate_ns_pond.merge import MergeOutcome, merge_jsonl_files


def _write_jsonl(path: Path, records: list[dict[str, str | None]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def test_merge_jsonl_files_merges_overlapping_records(tmp_path: Path) -> None:
    file_a = tmp_path / "a.jsonl"
    file_b = tmp_path / "b.jsonl"

    _write_jsonl(file_a, [{"pmid": "123456", "doi": None, "pmcid": None}])
    _write_jsonl(file_b, [{"pmid": "123456", "pmcid": "PMC123456", "doi": "10.1000/XYZ"}])

    outcome_ab = merge_jsonl_files([file_a, file_b])
    outcome_ba = merge_jsonl_files([file_b, file_a])

    assert isinstance(outcome_ab, MergeOutcome)
    assert outcome_ab.input_hash == outcome_ba.input_hash
    assert outcome_ab.source_files == [str(file_a), str(file_b)]

    assert len(outcome_ab.records) == 1
    record = outcome_ab.records[0]
    assert record["pmid"] == "123456"
    assert record["pmcid"] == "PMC123456"
    assert record["doi"] == "10.1000/xyz"
    assert not outcome_ab.errors


def test_merge_jsonl_records_warn_on_conflicts(tmp_path: Path) -> None:
    file_path = tmp_path / "conflict.jsonl"
    _write_jsonl(
        file_path,
        [
            {"pmid": "123", "doi": "10.1/A"},
            {"pmid": "123", "doi": "10.1/B"},
        ],
    )

    outcome = merge_jsonl_files([file_path])

    assert len(outcome.records) == 1
    record = outcome.records[0]
    assert record["pmid"] == "123"
    assert record["doi"] in {"10.1/a", "10.1/b"}
    assert outcome.errors
    assert any("doi" in message for message in outcome.errors)


def test_merge_jsonl_keeps_disconnected_records_separate(tmp_path: Path) -> None:
    file_path = tmp_path / "disconnected.jsonl"
    _write_jsonl(
        file_path,
        [
            {"pmid": "111"},
            {"pmcid": "PMC222"},
        ],
    )

    outcome = merge_jsonl_files([file_path])

    assert len(outcome.records) == 2
    values = {tuple(sorted(record.items())) for record in outcome.records}
    assert (('doi', None), ('pmcid', None), ('pmid', '111')) in values
    assert (('doi', None), ('pmcid', 'PMC222'), ('pmid', None)) in values


def test_merge_jsonl_skips_rows_without_identifiers(tmp_path: Path) -> None:
    file_path = tmp_path / "mixed.jsonl"
    _write_jsonl(
        file_path,
        [
            {"pmid": "999"},
            {"title": "No identifiers"},
            {"pmcid": "PMC999"},
        ],
    )

    outcome = merge_jsonl_files([file_path])

    assert len(outcome.records) == 2
    assert len(outcome.errors) == 1
    assert "no recognizable identifiers" in outcome.errors[0].lower()
