from pathlib import Path

import pytest

from curate_ns_pond.storage import (
    build_hashed_output_dir,
    hash_file_contents,
    hash_identifiers,
)


def test_hash_identifiers_is_stable() -> None:
    identifiers = ["pmid:123", "doi:10.1000/xyz"]
    first = hash_identifiers(identifiers)
    second = hash_identifiers(list(reversed(identifiers)))

    assert first == second
    assert len(first) == 16


def test_build_hashed_output_dir_creates_nested_dir(tmp_path: Path) -> None:
    output_dir = build_hashed_output_dir(tmp_path, ["a", "b"])

    assert output_dir.parent == tmp_path
    assert output_dir.exists()


def test_build_hashed_output_dir_raises_on_empty_list(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        build_hashed_output_dir(tmp_path, [])


def test_hash_file_contents_stable(tmp_path: Path) -> None:
    file_a = tmp_path / "a.jsonl"
    file_b = tmp_path / "b.jsonl"
    file_a.write_text("hello\n")
    file_b.write_text("world\n")

    first = hash_file_contents([file_a, file_b])
    second = hash_file_contents([file_b, file_a])

    assert first == second
    assert len(first) == 16
