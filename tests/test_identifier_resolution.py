from __future__ import annotations

import pytest

from curate_ns_pond.resolution import (
    IdentifierResolver,
    IdentifierKind,
    NormalizedIdentifier,
    ResolutionResult,
    normalize_identifier,
)
from curate_ns_pond.services.entrez import EntrezError, EntrezSummaryClient
from curate_ns_pond.services.pmc import PMCError, PMCIdConverter
from curate_ns_pond.services.semantic_scholar import (
    SemanticScholarClient,
    SemanticScholarError,
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


@pytest.mark.vcr
def test_resolver_merges_sources() -> None:
    with IdentifierResolver() as resolver:
        normalized = [
            normalize_identifier("32256646"),
            normalize_identifier("PMC7086438"),
            normalize_identifier("10.1155/2020/4598217"),
        ]
        result = resolver.resolve(normalized)

    assert isinstance(result, ResolutionResult)
    assert len(result.records) == 1
    record = result.records[0]
    assert record.pmid == "32256646"
    assert record.pmcid == "PMC7086438"
    assert record.doi == "10.1155/2020/4598217"
    assert result.sources_used == {"semantic-scholar", "pmc", "entrez"}
    assert result.errors == []


@pytest.mark.vcr
def test_resolver_records_errors() -> None:
    class ErrorSemanticScholarClient(SemanticScholarClient):
        def fetch_external_ids(self, identifier: str) -> dict[str, str] | None:
            assert self._client is not None
            response = self._client.get("https://httpbin.org/status/500")
            if response.status_code >= 400:
                raise SemanticScholarError("semantic-scholar: forced failure")
            return None

    class ErrorEntrezClient(EntrezSummaryClient):
        def fetch_article_ids(self, pmids):  # type: ignore[override]
            assert self._client is not None
            response = self._client.get("https://httpbin.org/status/500")
            if response.status_code >= 400:
                raise EntrezError("entrez: forced failure")
            return {}

    class ErrorPMCConverter(PMCIdConverter):
        def convert(self, pmcids):  # type: ignore[override]
            assert self._client is not None
            response = self._client.get("https://httpbin.org/status/500")
            if response.status_code >= 400:
                raise PMCError("pmc: forced failure")
            return {}

    semantic = ErrorSemanticScholarClient()
    entrez = ErrorEntrezClient()
    pmc = ErrorPMCConverter()

    try:
        with IdentifierResolver(
            semantic_scholar=semantic,
            entrez=entrez,
            pmc_converter=pmc,
        ) as resolver:
            normalized = [
                normalize_identifier("32256646"),
                normalize_identifier("PMC7086438"),
            ]
            result = resolver.resolve(normalized)
    finally:
        semantic.close()
        entrez.close()
        pmc.close()

    assert len(result.records) == 2
    pmid_record = next(record for record in result.records if record.pmid == "32256646")
    pmcid_record = next(record for record in result.records if record.pmcid == "PMC7086438")
    assert pmid_record.pmid == "32256646"
    assert pmcid_record.pmcid == "PMC7086438"
    assert "semantic-scholar" in " ".join(result.errors)
    assert "pmc" in " ".join(result.errors)
    assert "entrez" in " ".join(result.errors)
