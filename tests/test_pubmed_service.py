from __future__ import annotations

import pytest

from curate_ns_pond.services.pubmed import PubMedError, PubMedSearchService


@pytest.mark.vcr
def test_service_fetches_multiple_pages(vcr) -> None:
    service = PubMedSearchService(retmax=1)
    results = service.search_pmids("31452104[pmid] OR 31722068[pmid]")

    assert len(results) == 2
    assert set(results) == {"31722068", "31452104"}
    assert len(vcr.requests) == 2


@pytest.mark.vcr
def test_service_raises_for_http_error() -> None:
    service = PubMedSearchService(retmax=1)
    service.BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/invalid.fcgi"

    with pytest.raises(PubMedError):
        service.search_pmids("brain imaging")
