"""Utilities for querying the PubMed API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional

import httpx

__all__ = ["PubMedError", "PubMedSearchService"]


class PubMedError(RuntimeError):
    """Raised when a PubMed request fails."""


@dataclass(slots=True)
class PubMedSearchService:
    """High-level client for retrieving PubMed identifiers."""

    api_key: str | None = None
    email: str | None = None
    tool: str = "CurateNSPond"
    retmax: int = 1000
    timeout: float = 30.0
    _client: httpx.Client | None = None

    BASE_URL: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    def _get_client(self) -> tuple[httpx.Client, bool]:
        if self._client is not None:
            return self._client, False
        client = httpx.Client(timeout=self.timeout)
        return client, True

    def _build_params(
        self,
        query: str,
        start_date: date | None,
        end_date: date | None,
        retstart: int,
    ) -> dict[str, str]:
        params: dict[str, str] = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": str(self.retmax),
            "retstart": str(retstart),
            "tool": self.tool,
        }
        if self.api_key:
            params["api_key"] = self.api_key
        if self.email:
            params["email"] = self.email
        if start_date is not None or end_date is not None:
            params["datetype"] = "pdat"
        if start_date is not None:
            params["mindate"] = start_date.isoformat()
        if end_date is not None:
            params["maxdate"] = end_date.isoformat()
        return params

    def search_pmids(
        self,
        query: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[str]:
        client, should_close = self._get_client()
        try:
            retstart = 0
            total_count: Optional[int] = None
            pmids: list[str] = []

            while True:
                params = self._build_params(query, start_date, end_date, retstart)
                try:
                    response = client.get(self.BASE_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                except (httpx.HTTPError, ValueError) as exc:  # ValueError covers invalid JSON
                    raise PubMedError("Failed to query PubMed") from exc

                esearch = data.get("esearchresult")
                if not isinstance(esearch, dict):
                    raise PubMedError("Unexpected PubMed response structure")

                idlist = esearch.get("idlist", [])
                if not isinstance(idlist, Iterable):
                    raise PubMedError("Unexpected PubMed id list structure")

                batch = [str(identifier) for identifier in idlist]
                pmids.extend(batch)

                if total_count is None:
                    try:
                        total_count = int(esearch.get("count", len(pmids)))
                    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                        raise PubMedError("Invalid count in PubMed response") from exc

                if not batch:
                    break

                retstart += len(batch)
                if retstart >= total_count:
                    break

            return pmids
        finally:
            if should_close:
                client.close()
