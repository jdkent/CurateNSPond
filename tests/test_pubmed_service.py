from __future__ import annotations

import httpx
import pytest
import respx

from curate_ns_pond.services.pubmed import PubMedError, PubMedSearchService


@respx.mock
def test_service_fetches_multiple_pages() -> None:
    route = respx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi")
    payloads = [
        {
            "esearchresult": {
                "count": "5",
                "idlist": ["1", "2", "3"],
            }
        },
        {
            "esearchresult": {
                "count": "5",
                "idlist": ["4", "5"],
            }
        },
    ]
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if calls == 0:
            assert request.url.params["retstart"] == "0"
        else:
            assert request.url.params["retstart"] == "3"
        response = httpx.Response(200, json=payloads[calls])
        calls += 1
        return response

    route.mock(side_effect=handler)

    service = PubMedSearchService(retmax=3)
    results = service.search_pmids("brain imaging")

    assert results == ["1", "2", "3", "4", "5"]
    assert calls == 2


@respx.mock
def test_service_raises_for_http_error() -> None:
    respx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi").mock(
        return_value=httpx.Response(500)
    )

    service = PubMedSearchService()

    with pytest.raises(PubMedError):
        service.search_pmids("brain imaging")
