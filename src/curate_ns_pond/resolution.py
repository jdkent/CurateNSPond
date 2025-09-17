"""Identifier resolution workflow."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from .services.entrez import EntrezError, EntrezSummaryClient
from .services.pmc import PMCError, PMCIdConverter
from .services.semantic_scholar import SemanticScholarClient, SemanticScholarError


class IdentifierKind(str, Enum):
    PMID = "pmid"
    PMCID = "pmcid"
    DOI = "doi"


@dataclass(frozen=True, slots=True)
class NormalizedIdentifier:
    kind: IdentifierKind
    value: str
    original: str

    @property
    def hash_component(self) -> str:
        return f"{self.kind.value}:{self.value}"


@dataclass(slots=True)
class ResolvedRecord:
    pmid: str | None = None
    pmcid: str | None = None
    doi: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {"pmid": self.pmid, "pmcid": self.pmcid, "doi": self.doi}


@dataclass(slots=True)
class ResolutionResult:
    records: list[ResolvedRecord]
    sources_used: set[str]
    errors: list[str]
    started_at: datetime


_PMC_PREFIX = "PMC"


def normalize_pmid(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError("Empty PMID provided")
    if not candidate.isdigit():
        raise ValueError(f"Invalid PMID: {value}")
    return candidate


def normalize_pmcid(value: str) -> str:
    candidate = value.strip().upper()
    if candidate.startswith("PMCID:"):
        candidate = candidate.split(":", 1)[1]
    if not candidate.startswith(_PMC_PREFIX):
        candidate = f"{_PMC_PREFIX}{candidate}"
    number_part = candidate[len(_PMC_PREFIX) :]
    if not number_part.isdigit():
        raise ValueError(f"Invalid PMCID: {value}")
    return f"{_PMC_PREFIX}{number_part}"


def normalize_doi(value: str) -> str:
    candidate = value.strip()
    if candidate.lower().startswith("doi:"):
        candidate = candidate.split(":", 1)[1]
    if "/" not in candidate:
        raise ValueError(f"Invalid DOI: {value}")
    return candidate.lower()


def normalize_identifier(value: str) -> NormalizedIdentifier:
    raw = value.strip()
    if not raw:
        raise ValueError("Identifier cannot be blank")
    lowered = raw.lower()
    if lowered.startswith("pmid:"):
        raw = raw.split(":", 1)[1]
        return NormalizedIdentifier(IdentifierKind.PMID, normalize_pmid(raw), value)
    if lowered.startswith("pmcid:") or lowered.startswith("pmc"):
        return NormalizedIdentifier(IdentifierKind.PMCID, normalize_pmcid(raw), value)
    if raw.isdigit():
        return NormalizedIdentifier(IdentifierKind.PMID, normalize_pmid(raw), value)
    if "/" in raw:
        return NormalizedIdentifier(IdentifierKind.DOI, normalize_doi(raw), value)
    raise ValueError(f"Unrecognized identifier: {value}")


_ATTR_KIND = {
    "pmid": IdentifierKind.PMID,
    "pmcid": IdentifierKind.PMCID,
    "doi": IdentifierKind.DOI,
}


def _normalize_value(kind: IdentifierKind, value: str) -> str:
    if kind is IdentifierKind.PMID:
        return normalize_pmid(value)
    if kind is IdentifierKind.PMCID:
        return normalize_pmcid(value)
    if kind is IdentifierKind.DOI:
        return normalize_doi(value)
    raise ValueError(f"Unsupported identifier kind: {kind}")


class _ResolutionState:
    def __init__(self) -> None:
        self.records: list[ResolvedRecord] = []
        self.index: dict[tuple[IdentifierKind, str], ResolvedRecord] = {}

    def ensure_record(self, kind: IdentifierKind, value: str) -> ResolvedRecord:
        normalized = _normalize_value(kind, value)
        key = (kind, normalized)
        existing = self.index.get(key)
        if existing is not None:
            return existing
        record = ResolvedRecord()
        self.records.append(record)
        self._attach(record, kind, normalized)
        return record

    def link(self, record: ResolvedRecord, kind: IdentifierKind, value: str) -> ResolvedRecord:
        normalized = _normalize_value(kind, value)
        key = (kind, normalized)
        existing = self.index.get(key)
        if existing is None:
            self._attach(record, kind, normalized)
            self.index[key] = record
            return record
        if existing is record:
            return record
        merged = self._merge(existing, record)
        self._attach(merged, kind, normalized)
        self.index[key] = merged
        return merged

    def _merge(self, target: ResolvedRecord, other: ResolvedRecord) -> ResolvedRecord:
        if target is other:
            return target
        if other in self.records:
            self.records.remove(other)
        for attr in ("pmid", "pmcid", "doi"):
            value = getattr(other, attr)
            if value and not getattr(target, attr):
                setattr(target, attr, value)
        for attr, kind in _ATTR_KIND.items():
            value = getattr(target, attr)
            if value:
                self.index[(kind, _normalize_value(kind, value))] = target
        return target

    def _attach(self, record: ResolvedRecord, kind: IdentifierKind, value: str) -> None:
        if kind is IdentifierKind.PMID:
            record.pmid = value
        elif kind is IdentifierKind.PMCID:
            record.pmcid = value
        elif kind is IdentifierKind.DOI:
            record.doi = value
        else:  # pragma: no cover - defensive
            raise ValueError(f"Unsupported identifier kind: {kind}")
        self.index[(kind, value)] = record


class IdentifierResolver:
    """Resolve PMIDs, PMCIDs, and DOIs into unified records."""

    def __init__(
        self,
        *,
        semantic_scholar: SemanticScholarClient | None = None,
        entrez: EntrezSummaryClient | None = None,
        pmc_converter: PMCIdConverter | None = None,
    ) -> None:
        self._semantic = semantic_scholar or SemanticScholarClient()
        self._entrez = entrez or EntrezSummaryClient()
        self._pmc = pmc_converter or PMCIdConverter()
        self._owns_semantic = semantic_scholar is None
        self._owns_entrez = entrez is None
        self._owns_pmc = pmc_converter is None

    def close(self) -> None:
        if self._owns_semantic:
            self._semantic.close()
        if self._owns_entrez:
            self._entrez.close()
        if self._owns_pmc:
            self._pmc.close()

    def __enter__(self) -> "IdentifierResolver":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.close()

    def resolve_from_strings(self, identifiers: Sequence[str]) -> ResolutionResult:
        normalized = [normalize_identifier(item) for item in identifiers]
        return self.resolve(normalized)

    def resolve(self, identifiers: Sequence[NormalizedIdentifier]) -> ResolutionResult:
        state = _ResolutionState()
        sources_used: set[str] = set()
        errors: list[str] = []
        started_at = datetime.utcnow()

        for identifier in identifiers:
            state.ensure_record(identifier.kind, identifier.value)

        pmcids = sorted({ident.value for ident in identifiers if ident.kind is IdentifierKind.PMCID})
        if pmcids:
            try:
                conversions = self._pmc.convert(pmcids)
                if conversions:
                    sources_used.add("pmc")
                for pmcid, payload in conversions.items():
                    record = state.ensure_record(IdentifierKind.PMCID, pmcid)
                    pmid = payload.get("pmid")
                    if pmid:
                        record = state.link(record, IdentifierKind.PMID, pmid)
                    doi = payload.get("doi")
                    if doi:
                        record = state.link(record, IdentifierKind.DOI, doi)
            except PMCError as exc:
                errors.append(str(exc))

        pmids = sorted({
            record.pmid
            for record in state.records
            if record.pmid
        })
        if pmids:
            try:
                summaries = self._entrez.fetch_article_ids(pmids)
                if summaries:
                    sources_used.add("entrez")
                for pmid, payload in summaries.items():
                    record = state.ensure_record(IdentifierKind.PMID, pmid)
                    pmcid = payload.get("pmcid")
                    if pmcid:
                        record = state.link(record, IdentifierKind.PMCID, pmcid)
                    doi = payload.get("doi")
                    if doi:
                        state.link(record, IdentifierKind.DOI, doi)
            except EntrezError as exc:
                errors.append(str(exc))

        semantic_targets = sorted({
            (IdentifierKind.PMID, record.pmid)
            for record in state.records
            if record.pmid
        } |
            {
                (IdentifierKind.DOI, record.doi)
                for record in state.records
                if record.doi
            }
        )
        for kind, value in semantic_targets:
            if not value:
                continue
            try:
                data = self._semantic.fetch_external_ids(value)
            except SemanticScholarError as exc:
                errors.append(str(exc))
                continue
            if not data:
                continue
            sources_used.add("semantic-scholar")
            record = state.ensure_record(kind, value)
            pmid = data.get("pmid")
            if pmid:
                record = state.link(record, IdentifierKind.PMID, pmid)
            pmcid = data.get("pmcid")
            if pmcid:
                record = state.link(record, IdentifierKind.PMCID, pmcid)
            doi = data.get("doi")
            if doi:
                state.link(record, IdentifierKind.DOI, doi)

        return ResolutionResult(records=state.records.copy(), sources_used=sources_used, errors=errors, started_at=started_at)
