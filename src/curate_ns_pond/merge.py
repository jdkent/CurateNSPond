"""Utilities for merging identifier records from JSONL files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Sequence

from .resolution import IdentifierKind, NormalizedIdentifier, normalize_identifier
from .storage import hash_file_contents

__all__ = ["MergeOutcome", "merge_jsonl_files"]


@dataclass(slots=True)
class MergeOutcome:
    """Result of merging one or more JSONL files."""

    records: list[dict[str, str | None]]
    input_hash: str
    source_files: list[str]
    errors: list[str]


def _read_jsonl(path: Path, errors: list[str]) -> list[dict[str, str | None]]:
    records: list[dict[str, str | None]] = []
    with path.open("r", encoding="utf-8") as handle:
        for idx, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"{path}:{idx}: invalid JSON ({exc.msg})")
                continue
            if not isinstance(payload, dict):
                errors.append(f"{path}:{idx}: expected JSON object per line")
                continue
            record = {
                "pmid": payload.get("pmid"),
                "pmcid": payload.get("pmcid"),
                "doi": payload.get("doi"),
            }
            if not any(record.values()):
                errors.append(f"{path}:{idx}: row has no recognizable identifiers")
                continue
            records.append(record)
    return records


def _group_identifiers(records: Iterable[list[NormalizedIdentifier]]) -> dict[str, Dict[IdentifierKind, set[str]]]:
    parent: dict[str, str] = {}
    key_to_identifier: dict[str, NormalizedIdentifier] = {}

    def find(key: str) -> str:
        parent.setdefault(key, key)
        if parent[key] != key:
            parent[key] = find(parent[key])
        return parent[key]

    def union(a: str, b: str) -> None:
        root_a = find(a)
        root_b = find(b)
        if root_a != root_b:
            parent[root_b] = root_a

    for identifiers in records:
        if not identifiers:
            continue
        keys = [identifier.hash_component for identifier in identifiers]
        for key, identifier in zip(keys, identifiers):
            key_to_identifier.setdefault(key, identifier)
            find(key)
        first_key = keys[0]
        for key in keys[1:]:
            union(first_key, key)

    components: dict[str, Dict[IdentifierKind, set[str]]] = {}
    for key, identifier in key_to_identifier.items():
        root = find(key)
        bucket = components.setdefault(root, {kind: set() for kind in IdentifierKind})
        bucket[identifier.kind].add(identifier.value)

    return components


def merge_jsonl_files(paths: Sequence[Path]) -> MergeOutcome:
    if not paths:
        raise ValueError("At least one JSONL file is required")

    errors: list[str] = []
    normalized_records: list[list[NormalizedIdentifier]] = []

    ordered_paths = sorted(paths, key=lambda item: item.as_posix())
    for path in ordered_paths:
        extracted = _read_jsonl(path, errors)
        for record in extracted:
            identifiers: list[NormalizedIdentifier] = []
            for key in ("pmid", "pmcid", "doi"):
                value = record.get(key)
                if value is None or (isinstance(value, str) and not value.strip()):
                    continue
                try:
                    identifiers.append(normalize_identifier(str(value)))
                except ValueError as exc:
                    errors.append(f"{path}: {exc}")
            if identifiers:
                normalized_records.append(identifiers)
            else:
                errors.append(
                    f"{path}: record discarded after normalization due to missing identifiers"
                )

    components = _group_identifiers(normalized_records)

    sortable_records: list[tuple[str, dict[str, str | None]]] = []

    for root, values in components.items():
        record: dict[str, str | None] = {"pmid": None, "pmcid": None, "doi": None}
        sort_tokens: list[str] = []
        for kind in IdentifierKind:
            entries = sorted(values[kind])
            if not entries:
                continue
            if len(entries) > 1:
                errors.append(
                    f"Conflicting {kind.value} values encountered: {entries}"
                )
            chosen = entries[0]
            record[kind.value] = chosen
            sort_tokens.append(f"{kind.value}:{chosen}")
        sort_key = sort_tokens[0] if sort_tokens else root
        sortable_records.append((sort_key, record))

    sortable_records.sort(key=lambda item: item[0])
    merged_records = [record for _, record in sortable_records]

    outcome = MergeOutcome(
        records=merged_records,
        input_hash=hash_file_contents(paths),
        source_files=[str(path) for path in ordered_paths],
        errors=errors,
    )
    return outcome
