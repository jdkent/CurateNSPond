from __future__ import annotations

import httpx
import pytest
import respx

from curate_ns_pond.resolution import (
    IdentifierResolver,
    IdentifierKind,
    NormalizedIdentifier,
    ResolutionResult,
    normalize_identifier,
)


def test_normalize_identifier_variants() -> None:
    pmid = normalize_identifier("123456")
    assert pmid.kind is IdentifierKind.PMID
    assert pmid.value == "123456"

    pmcid = normalize_identifier("pmc1234")
    assert pmcid.kind is IdentifierKind.PMCID
    assert pmcid.value == "PMC1234"

    pmcid_with_prefix = normalize_identifier("pmcid:PMC999")
    assert pmcid_with_prefix.kind is IdentifierKind.PMCID
    assert pmcid_with_prefix.value == "PMC999"

    doi = normalize_identifier("10.1000/ABC")
    assert doi.kind is IdentifierKind.DOI
    assert doi.value == "10.1000/abc"

    with pytest.raises(ValueError):
        normalize_identifier("")

    with pytest.raises(ValueError):
        normalize_identifier("not an id")


@respx.mock
def test_resolver_merges_sources() -> None:
    pmc_route = respx.get("https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/")

    def pmc_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["ids"] == "PMC123456"
        payload = {
            "records": [
                {
                    "pmcid": "PMC123456",
                    "pmid": "123456",
                    "doi": "10.1000/xyz",
                }
            ]
        }
        return httpx.Response(200, json=payload)

    pmc_route.mock(side_effect=pmc_handler)

    entrez_route = respx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi")

    def entrez_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["id"] == "123456"
        payload = {
            "result": {
                "uids": ["123456"],
                "123456": {
                    "articleids": [
                        {"idtype": "pmid", "value": "123456"},
                        {"idtype": "pmcid", "value": "PMC123456"},
                        {"idtype": "doi", "value": "10.1000/xyz"},
                    ]
                },
            }
        }
        return httpx.Response(200, json=payload)

    entrez_route.mock(side_effect=entrez_handler)

    s2_route = respx.get("https://api.semanticscholar.org/graph/v1/paper/123456")
    s2_route.mock(
        return_value=httpx.Response(
            200,
            json={
                "externalIds": {
                    "DOI": "10.1000/xyz",
                    "PMCID": "PMC123456",
                    "PMID": "123456",
                }
            },
        )
    )

    doi_route = respx.get(
        "https://api.semanticscholar.org/graph/v1/paper/10.1000%2Fxyz"
    )
    doi_route.mock(
        return_value=httpx.Response(
            200,
            json={
                "externalIds": {
                    "DOI": "10.1000/xyz",
                    "PMCID": "PMC123456",
                    "PMID": "123456",
                }
            },
        )
    )

    with IdentifierResolver() as resolver:
        normalized = [
            normalize_identifier("123456"),
            normalize_identifier("PMC123456"),
            normalize_identifier("10.1000/xyz"),
        ]
        result = resolver.resolve(normalized)

    assert isinstance(result, ResolutionResult)
    assert len(result.records) == 1
    record = result.records[0]
    assert record.pmid == "123456"
    assert record.pmcid == "PMC123456"
    assert record.doi == "10.1000/xyz"
    assert result.sources_used == {"semantic-scholar", "pmc", "entrez"}
    assert result.errors == []


@respx.mock
def test_resolver_records_errors() -> None:
    respx.get("https://api.semanticscholar.org/graph/v1/paper/123456").mock(
        return_value=httpx.Response(500)
    )
    respx.get("https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/").mock(
        return_value=httpx.Response(200, json={"records": []})
    )
    respx.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi").mock(
        return_value=httpx.Response(200, json={"result": {"uids": []}})
    )

    with IdentifierResolver() as resolver:
        normalized = [normalize_identifier("123456")]
        result = resolver.resolve(normalized)

    assert len(result.records) == 1
    assert result.records[0].pmid == "123456"
    assert "semantic-scholar" in " ".join(result.errors)
