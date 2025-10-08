"""Client helpers for PubMed Entrez summaries."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

import httpx


class EntrezError(RuntimeError):
    """Raised when Entrez requests fail."""


@dataclass(slots=True)
class EntrezSummaryClient:
    email: str | None = None
    tool: str = "CurateNSPond"
    timeout: float = 30.0
    _client: httpx.Client | None = None

    BASE_URL: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    def __post_init__(self) -> None:
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def _fetch_summary(self, pmids: Iterable[str]) -> dict[str, dict[str, object]]:
        pmid_list = [str(pmid) for pmid in pmids if pmid]
        if not pmid_list:
            return {}
        params = {
            "db": "pubmed",
            "id": ",".join(pmid_list),
            "retmode": "json",
            "tool": self.tool,
        }
        if self.email:
            params["email"] = self.email
        try:
            response = self._client.get(self.BASE_URL, params=params)
        except httpx.HTTPError as exc:  # pragma: no cover - defensive
            raise EntrezError("entrez: request failed") from exc
        if response.status_code >= 400:
            raise EntrezError(f"entrez: error {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:  # pragma: no cover - defensive
            raise EntrezError("entrez: invalid JSON response") from exc
        result = payload.get("result")
        if not isinstance(result, dict):
            return {}
        summaries: dict[str, dict[str, object]] = {}
        for pmid in pmid_list:
            entry = result.get(pmid)
            if isinstance(entry, dict):
                summaries[pmid] = entry
        return summaries

    def fetch_article_ids(self, pmids: Iterable[str]) -> dict[str, dict[str, str]]:
        raw_summaries = self._fetch_summary(pmids)
        summaries: dict[str, dict[str, str]] = {}
        for pmid, entry in raw_summaries.items():
            article_ids = entry.get("articleids", [])
            if not isinstance(article_ids, list):
                continue
            record: dict[str, str] = {}
            for item in article_ids:
                if not isinstance(item, dict):
                    continue
                idtype = str(item.get("idtype", "")).lower()
                value = item.get("value")
                if not value:
                    continue
                if idtype in {"pmcid", "pmc"}:
                    normalized = self._normalize_pmcid(str(value))
                    if normalized:
                        record["pmcid"] = normalized
                elif idtype == "doi":
                    record["doi"] = str(value)
            if record:
                summaries[pmid] = record
        return summaries

    @staticmethod
    def _normalize_pmcid(value: str) -> str | None:
        candidate = value.strip()
        if not candidate:
            return None
        match = re.search(r"PMC\d+", candidate.upper())
        if match:
            return match.group(0)
        if candidate.upper().startswith("PMC"):
            return candidate.upper()
        return None

    def fetch_metadata(self, pmid: str) -> dict[str, object] | None:
        summaries = self._fetch_summary([pmid])
        entry = summaries.get(pmid)
        if not entry:
            return None
        authors = []
        raw_authors = entry.get("authors", [])
        if isinstance(raw_authors, list):
            for author in raw_authors:
                if isinstance(author, dict) and author.get("name"):
                    authors.append(str(author["name"]))
        metadata: dict[str, object] = {
            "title": entry.get("title"),
            "abstract": entry.get("elocationid"),
            "authors": authors,
            "journal": entry.get("fulljournalname"),
            "year": entry.get("pubdate") or entry.get("sortpubdate"),
        }
        return metadata
