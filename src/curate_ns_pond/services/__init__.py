"""Service layer for interacting with external resources."""

from .entrez import EntrezError, EntrezSummaryClient
from .pmc import PMCError, PMCIdConverter
from .pubmed import PubMedError, PubMedSearchService
from .semantic_scholar import SemanticScholarClient, SemanticScholarError

__all__ = [
    "EntrezError",
    "EntrezSummaryClient",
    "PMCError",
    "PMCIdConverter",
    "PubMedError",
    "PubMedSearchService",
    "SemanticScholarClient",
    "SemanticScholarError",
]
