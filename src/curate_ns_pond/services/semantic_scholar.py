"""Client for Semantic Scholar identifiers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx


class SemanticScholarError(RuntimeError):
    """Raised when Semantic Scholar data cannot be retrieved."""


@dataclass(slots=True)
class SemanticScholarClient:
    api_key: str | None = None
    timeout: float = 30.0
    _client: httpx.Client | None = None

    BASE_URL: str = "https://api.semanticscholar.org/graph/v1"

    def __post_init__(self) -> None:
        if self._client is None:
            headers = {"User-Agent": "CurateNSPond/identifier-resolver"}
            if self.api_key:
                headers["x-api-key"] = self.api_key
            self._client = httpx.Client(timeout=self.timeout, headers=headers)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def fetch_external_ids(self, identifier: str) -> dict[str, str] | None:
        assert self._client is not None  # for mypy
        safe_identifier = quote(identifier, safe="")
        url = f"{self.BASE_URL}/paper/{safe_identifier}"
        try:
            response = self._client.get(url, params={"fields": "externalIds"})
        except httpx.HTTPError as exc:  # pragma: no cover - network failure
            raise SemanticScholarError(f"semantic-scholar: failed to fetch {identifier}") from exc
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise SemanticScholarError(
                f"semantic-scholar: error {response.status_code} for {identifier}"
            )
        try:
            payload = response.json()
        except ValueError as exc:  # pragma: no cover - defensive
            raise SemanticScholarError("semantic-scholar: invalid JSON response") from exc
        external_ids = payload.get("externalIds")
        if not isinstance(external_ids, dict):
            return None
        result: dict[str, str] = {}
        for key in ("PMID", "pmid"):
            if key in external_ids and external_ids[key]:
                result["pmid"] = str(external_ids[key])
                break
        for key in ("PMCID", "pmcid"):
            if key in external_ids and external_ids[key]:
                result["pmcid"] = str(external_ids[key])
                break
        for key in ("DOI", "doi"):
            if key in external_ids and external_ids[key]:
                result["doi"] = str(external_ids[key])
                break
        return result if result else None

    def fetch_metadata(self, identifier: str) -> dict[str, Any] | None:
        assert self._client is not None  # for mypy
        safe_identifier = quote(identifier, safe="")
        fields = "title,abstract,authors,venue,journal,year"
        url = f"{self.BASE_URL}/paper/{safe_identifier}"
        try:
            response = self._client.get(url, params={"fields": fields})
        except httpx.HTTPError as exc:  # pragma: no cover - network failure
            raise SemanticScholarError(f"semantic-scholar: failed to fetch {identifier}") from exc
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise SemanticScholarError(
                f"semantic-scholar: error {response.status_code} for {identifier}"
            )
        try:
            payload = response.json()
        except ValueError as exc:  # pragma: no cover - defensive
            raise SemanticScholarError("semantic-scholar: invalid JSON response") from exc
        result: dict[str, Any] = {}
        for key in ("title", "abstract", "venue", "journal", "year"):
            if key in payload:
                result[key] = payload[key]
        authors = payload.get("authors")
        if isinstance(authors, list):
            names = [author.get("name") for author in authors if isinstance(author, dict) and author.get("name")]
            result["authors"] = names
        return result or None
