"""Client for NCBI PMC identifier conversions."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import urlopen

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
        payload = self._fetch_payload(params)
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

    def _fetch_payload(self, params: dict[str, str]) -> dict[str, object]:
        try:
            response = self._client.get(self.BASE_URL, params=params, follow_redirects=True)
        except httpx.HTTPError as exc:  # pragma: no cover - network failure
            raise PMCError("pmc: request failed") from exc
        if response.status_code == 403:
            return self._fetch_with_urllib(params)
        if response.status_code >= 400:
            raise PMCError(f"pmc: error {response.status_code}")
        try:
            return response.json()
        except ValueError:
            return self._fetch_with_urllib(params)

    def _fetch_with_urllib(self, params: dict[str, str]) -> dict[str, object]:
        url = f"{self.BASE_URL}?{urlencode(params)}"
        try:
            with urlopen(url, timeout=self.timeout) as response:  # type: ignore[call-arg]
                return json.load(response)
        except Exception as exc:  # pragma: no cover - defensive
            raise PMCError("pmc: invalid JSON response") from exc
