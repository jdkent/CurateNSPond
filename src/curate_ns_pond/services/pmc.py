"""Client for NCBI PMC identifier conversions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import httpx


class PMCError(RuntimeError):
    """Raised when PMC identifier conversion fails."""


@dataclass(slots=True)
class PMCIdConverter:
    email: str | None = None
    tool: str = "CurateNSPond"
    timeout: float = 30.0
    _client: httpx.Client | None = None

    BASE_URL: str = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"

    def __post_init__(self) -> None:
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def convert(self, pmcids: Iterable[str]) -> dict[str, dict[str, str]]:
        pmcid_list = [str(pmcid) for pmcid in pmcids if pmcid]
        if not pmcid_list:
            return {}
        params = {
            "ids": ",".join(pmcid_list),
            "format": "json",
            "tool": self.tool,
        }
        if self.email:
            params["email"] = self.email
        try:
            response = self._client.get(self.BASE_URL, params=params)
        except httpx.HTTPError as exc:  # pragma: no cover - network failure
            raise PMCError("pmc: request failed") from exc
        if response.status_code >= 400:
            raise PMCError(f"pmc: error {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:  # pragma: no cover - defensive
            raise PMCError("pmc: invalid JSON response") from exc
        records = payload.get("records", [])
        if not isinstance(records, list):
            return {}
        result: dict[str, dict[str, str]] = {}
        for record in records:
            if not isinstance(record, dict):
                continue
            pmcid = record.get("pmcid")
            if not pmcid:
                continue
            entry: dict[str, str] = {}
            if record.get("pmid"):
                entry["pmid"] = str(record["pmid"])
            if record.get("doi"):
                entry["doi"] = str(record["doi"])
            result[str(pmcid)] = entry
        return result
