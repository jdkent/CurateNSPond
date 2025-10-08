"""Full text retrieval workflow integrating PubGet and ACE."""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from bs4 import BeautifulSoup

from .services.entrez import EntrezSummaryClient
from .services.semantic_scholar import SemanticScholarClient
from .settings import PipelineSettings
from .storage import hash_file_contents

__all__ = [
    "ACEClient",
    "BibliographicMetadata",
    "FullTextFetcher",
    "FullTextRecord",
    "FullTextResult",
    "PubGetClient",
]


class PubGetClient:
    """Thin wrapper around the ``pubget`` command-line interface."""

    def __init__(self, *, executable: str = "pubget") -> None:
        self.executable = executable

    def fetch_text(self, pmcid: str) -> str | None:
        """Fetch plain-text full text for the provided PMCID."""

        with tempfile.NamedTemporaryFile(suffix=".txt") as tmp:
            command = [
                self.executable,
                "fetch",
                pmcid,
                "--fulltext",
                "--output",
                tmp.name,
            ]
            try:
                result = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                )
            except FileNotFoundError as exc:  # pragma: no cover - depends on system
                raise RuntimeError(
                    "pubget executable not found; install the fulltext extra"
                ) from exc
            if result.returncode != 0:
                return None
            text = Path(tmp.name).read_text(encoding="utf-8").strip()
            return text or None


class ACEClient:
    """Wrapper around the ACE Python API for fetching HTML full text."""

    def __init__(
        self,
        *,
        executable: str | None = None,
        scraper: Any | None = None,
        metadata_fetcher: Any | None = None,
        temp_dir: str | Path | None = None,
        mode: str = "requests",
        prefer_pmc_source: bool | str = True,
    ) -> None:
        if executable and executable != "ace":  # pragma: no cover - compatibility shim
            warnings.warn(
                "ACEClient no longer shells out to 'ace'; the 'executable' argument is ignored",
                RuntimeWarning,
                stacklevel=2,
            )

        ace_scrape = self._load_ace_module()

        self._metadata_fetcher = metadata_fetcher or getattr(ace_scrape, "get_pubmed_metadata", None)
        if self._metadata_fetcher is None:
            raise RuntimeError("Invalid ACE installation: missing get_pubmed_metadata helper")

        self._temp_manager: tempfile.TemporaryDirectory[str] | None = None
        if scraper is not None:
            self._scraper = scraper
        else:
            store_path = self._ensure_store_path(temp_dir)
            self._scraper = ace_scrape.Scraper(store_path)

        self._mode = mode
        self._prefer_pmc_source = prefer_pmc_source

    @staticmethod
    def _load_ace_module() -> Any:
        try:
            import ace.scrape as ace_scrape
        except ImportError as exc:  # pragma: no cover - depends on extra being installed
            raise RuntimeError("ACE package not available; install the 'fulltext' extra") from exc
        return ace_scrape

    def _ensure_store_path(self, temp_dir: str | Path | None) -> str:
        if temp_dir is not None:
            return str(Path(temp_dir))
        self._temp_manager = tempfile.TemporaryDirectory()
        return self._temp_manager.name

    def fetch_text(self, pmid: str) -> str | None:
        journal = "unknown"
        metadata: dict[str, Any] | None = None
        try:
            metadata = self._metadata_fetcher(pmid)
        except Exception:  # pragma: no cover - defensive network call guard
            metadata = None

        if isinstance(metadata, dict):
            journal_value = metadata.get("journal") or metadata.get("source")
            if isinstance(journal_value, str) and journal_value.strip():
                journal = journal_value

        try:
            html = self._scraper.get_html_by_pmid(
                pmid,
                journal=journal,
                mode=self._mode,
                prefer_pmc_source=self._prefer_pmc_source,
            )
        except Exception as exc:  # pragma: no cover - relies on external service
            raise RuntimeError(f"ACE scrape failed for PMID {pmid}") from exc

        if not html:
            return None

        text = self._clean_html(html)
        return text or None

    @staticmethod
    def _clean_html(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        raw_text = soup.get_text(separator="\n")
        lines = [line.strip() for line in raw_text.splitlines()]
        filtered = "\n".join(line for line in lines if line)
        return filtered.strip()


@dataclass(slots=True)
class BibliographicMetadata:
    """Structured bibliographic information about a record."""

    title: str | None = None
    abstract: str | None = None
    authors: list[str] = field(default_factory=list)
    journal: str | None = None
    year: int | None = None
    source: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "abstract": self.abstract,
            "authors": self.authors,
            "journal": self.journal,
            "year": self.year,
            "source": self.source,
        }


@dataclass(slots=True)
class FullTextRecord:
    pmid: str | None
    pmcid: str | None
    doi: str | None
    text: str | None
    text_source: str | None
    metadata: BibliographicMetadata | None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "pmid": self.pmid,
            "pmcid": self.pmcid,
            "doi": self.doi,
            "text": self.text,
            "text_source": self.text_source,
        }
        data["metadata"] = self.metadata.to_dict() if self.metadata else None
        return data


@dataclass(slots=True)
class FullTextResult:
    records: list[FullTextRecord]
    errors: list[str]
    sources_used: set[str]
    metadata_sources_used: set[str]
    batch_hash: str
    input_files: list[str]
    started_at: datetime

    @property
    def success_count(self) -> int:
        return sum(1 for record in self.records if record.text)

    @property
    def failure_count(self) -> int:
        return sum(1 for record in self.records if not record.text)


class FullTextFetcher:
    """Coordinate full text retrieval via PubGet and ACE."""

    def __init__(
        self,
        *,
        settings: PipelineSettings | None = None,
        pubget_client: PubGetClient | None = None,
        ace_client: ACEClient | None = None,
        semantic_client: SemanticScholarClient | None = None,
        entrez_client: EntrezSummaryClient | None = None,
    ) -> None:
        self.settings = settings or PipelineSettings()
        self._pubget = pubget_client or PubGetClient()
        self._ace = ace_client or ACEClient()
        self._semantic = semantic_client or SemanticScholarClient()
        self._entrez = entrez_client or EntrezSummaryClient()

    def fetch_from_files(self, jsonl_paths: Sequence[Path]) -> FullTextResult:
        if not jsonl_paths:
            raise ValueError("At least one JSONL file must be provided")

        started_at = datetime.utcnow()
        errors: list[str] = []
        records = self._load_records(jsonl_paths, errors)

        self.settings.ensure_directories()
        batch_hash = hash_file_contents(jsonl_paths)
        run_dir = self.settings.processed_dir / "fulltext" / batch_hash
        records_dir = run_dir / "records"
        records_dir.mkdir(parents=True, exist_ok=True)

        fulltext_records: list[FullTextRecord] = []
        sources_used: set[str] = set()
        metadata_sources: set[str] = set()

        for record in records:
            text, source = self._fetch_text(record, errors)
            metadata: BibliographicMetadata | None = None
            if text:
                metadata = self._fetch_metadata(record, metadata_sources)
            fulltext_records.append(
                FullTextRecord(
                    pmid=record.get("pmid"),
                    pmcid=record.get("pmcid"),
                    doi=record.get("doi"),
                    text=text,
                    text_source=source,
                    metadata=metadata,
                )
            )
            if source:
                sources_used.add(source)

        for index, record in enumerate(fulltext_records):
            slug = self._record_slug(record, index)
            path = records_dir / f"{slug}.json"
            path.write_text(json.dumps(record.to_dict(), indent=2, sort_keys=True), encoding="utf-8")

        metadata_path = run_dir / "metadata.json"
        metadata_summary = {
            "input_files": [str(path) for path in jsonl_paths],
            "input_hash": batch_hash,
            "record_count": len(fulltext_records),
            "records_with_text": sum(1 for record in fulltext_records if record.text),
            "records_without_text": sum(1 for record in fulltext_records if not record.text),
            "text_sources": sorted(sources_used),
            "metadata_sources": sorted(metadata_sources),
            "errors": errors,
            "run_started_at": started_at.isoformat(timespec="seconds") + "Z",
            "records_dir": str(records_dir),
        }
        metadata_path.write_text(json.dumps(metadata_summary, indent=2, sort_keys=True), encoding="utf-8")

        return FullTextResult(
            records=fulltext_records,
            errors=errors,
            sources_used=sources_used,
            metadata_sources_used=metadata_sources,
            batch_hash=batch_hash,
            input_files=[str(path) for path in jsonl_paths],
            started_at=started_at,
        )

    def _load_records(self, paths: Sequence[Path], errors: list[str]) -> list[dict[str, str | None]]:
        unique: dict[tuple[str | None, str | None, str | None], dict[str, str | None]] = {}
        for path in sorted(paths, key=lambda item: item.as_posix()):
            with path.open("r", encoding="utf-8") as handle:
                for idx, raw_line in enumerate(handle, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError as exc:
                        errors.append(f"{path}:{idx}: invalid JSON ({exc.msg})")
                        continue
                    if not isinstance(payload, dict):
                        errors.append(f"{path}:{idx}: expected JSON object per line")
                        continue
                    pmid = payload.get("pmid")
                    pmcid = payload.get("pmcid")
                    doi = payload.get("doi")
                    if not any([pmid, pmcid, doi]):
                        errors.append(f"{path}:{idx}: record missing identifiers")
                        continue
                    key = (pmid, pmcid, doi)
                    unique.setdefault(key, {"pmid": pmid, "pmcid": pmcid, "doi": doi})
        return list(unique.values())

    def _fetch_text(self, record: dict[str, str | None], errors: list[str]) -> tuple[str | None, str | None]:
        pmcid = record.get("pmcid")
        pmid = record.get("pmid")

        if pmcid:
            try:
                text = self._pubget.fetch_text(pmcid)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"PubGet failed for {pmcid}: {exc}")
            else:
                if text:
                    return text, "pubget"
                errors.append(f"PubGet returned no text for {pmcid}")

        if pmid:
            try:
                text = self._ace.fetch_text(pmid)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"ACE failed for PMID {pmid}: {exc}")
                return None, None
            if text:
                return text, "ace"
            errors.append(f"ACE returned no text for PMID {pmid}")

        return None, None

    def _fetch_metadata(self, record: dict[str, str | None], metadata_sources: set[str]) -> BibliographicMetadata | None:
        identifiers = [identifier for identifier in (record.get("pmid"), record.get("doi")) if identifier]
        for identifier in identifiers:
            try:
                metadata = self._semantic.fetch_metadata(identifier)
            except Exception as exc:  # pragma: no cover - defensive
                continue
            if metadata:
                metadata_sources.add("semantic-scholar")
                return self._metadata_from_dict(metadata, "semantic-scholar")

        pmid = record.get("pmid")
        if not pmid:
            return None
        try:
            metadata = self._entrez.fetch_metadata(pmid)
        except Exception:  # pragma: no cover - defensive
            return None
        if metadata:
            metadata_sources.add("entrez")
            return self._metadata_from_dict(metadata, "entrez")
        return None

    @staticmethod
    def _metadata_from_dict(data: dict[str, Any], source: str) -> BibliographicMetadata:
        authors = data.get("authors") or []
        if isinstance(authors, dict):  # pragma: no cover - defensive
            authors = list(authors.values())
        if not isinstance(authors, list):
            authors = []
        authors_list: list[str] = []
        for author in authors:
            if isinstance(author, str):
                authors_list.append(author)
            elif isinstance(author, dict) and author.get("name"):
                authors_list.append(str(author["name"]))
            else:
                authors_list.append(str(author))
        year = data.get("year")
        if isinstance(year, str):
            match = re.search(r"(19|20)\d{2}", year)
            year_value = int(match.group()) if match else None
        elif isinstance(year, int):
            year_value = year
        else:
            year_value = None
        return BibliographicMetadata(
            title=_safe_str(data.get("title")),
            abstract=_safe_str(data.get("abstract")),
            authors=authors_list,
            journal=_safe_str(data.get("journal")) or _safe_str(data.get("venue")),
            year=year_value,
            source=source,
        )

    @staticmethod
    def _record_slug(record: FullTextRecord, index: int) -> str:
        identifier = (record.pmid or record.pmcid or record.doi or f"record-{index}").lower()
        slug = re.sub(r"[^a-z0-9_-]+", "_", identifier).strip("_")
        return f"{index:04d}_{slug or 'record'}"


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
