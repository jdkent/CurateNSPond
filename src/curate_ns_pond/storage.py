"""Filesystem helpers for organizing pipeline artifacts."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, Sequence

NORMALIZED_SEPARATOR = "\n"


def _normalize_identifiers(identifiers: Iterable[str]) -> list[str]:
    cleaned = [identifier.strip() for identifier in identifiers if identifier.strip()]
    if not cleaned:
        return []
    # Use sorted order so hashes are independent of original ordering.
    return sorted(cleaned)


def hash_identifiers(identifiers: Sequence[str]) -> str:
    """Return a deterministic short hash for a collection of identifiers."""

    normalized = _normalize_identifiers(identifiers)
    if not normalized:
        raise ValueError("at least one identifier is required to compute a hash")

    digest = hashlib.sha256(NORMALIZED_SEPARATOR.join(normalized).encode("utf-8"))
    return digest.hexdigest()[:16]


def build_hashed_output_dir(base_dir: Path, identifiers: Sequence[str]) -> Path:
    """Create (if needed) and return a hashed output directory under ``base_dir``."""

    hashed = hash_identifiers(identifiers)
    target_dir = base_dir / hashed
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def hash_file_contents(paths: Sequence[Path]) -> str:
    """Return a deterministic hash of the provided file contents."""

    hasher = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        if not path.exists():
            continue
        hasher.update(path.read_bytes())
    return hasher.hexdigest()[:16]
