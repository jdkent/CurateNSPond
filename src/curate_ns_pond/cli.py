"""Command line interface for CurateNSPond."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import typer

from .fulltext import FullTextFetcher
from .merge import merge_jsonl_files
from .resolution import IdentifierResolver, normalize_identifier
from .services.pubmed import PubMedSearchService
from .settings import PipelineSettings
from .storage import build_hashed_output_dir, hash_identifiers

app = typer.Typer(help="Utilities for the CurateNSPond pipeline.")

search_app = typer.Typer(help="Search utilities.")
resolve_app = typer.Typer(help="Identifier resolution utilities.")
merge_app = typer.Typer(help="Record merging utilities.")
fetch_app = typer.Typer(help="Full text retrieval utilities.")

app.add_typer(search_app, name="search")
app.add_typer(resolve_app, name="resolve")
app.add_typer(merge_app, name="merge")
app.add_typer(fetch_app, name="fetch")


def _parse_date(option_name: str, value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - Typer converts to a CLI error
        raise typer.BadParameter(f"Invalid date for {option_name}: {value}") from exc


@search_app.command("pubmed")
def search_pubmed(
    query: str = typer.Argument(..., help="PubMed search query."),
    start_date: Optional[str] = typer.Option(
        None, help="Restrict search to publications on/after this date (YYYY-MM-DD)."
    ),
    end_date: Optional[str] = typer.Option(
        None, help="Restrict search to publications on/before this date (YYYY-MM-DD)."
    ),
    retmax: int = typer.Option(1000, min=1, help="Number of records to request per page."),
    api_key: Optional[str] = typer.Option(None, help="NCBI API key for higher rate limits."),
    email: Optional[str] = typer.Option(None, help="Contact email sent to NCBI with requests."),
) -> None:
    """Run a PubMed search and persist PMIDs to the raw data directory."""

    parsed_start = _parse_date("start-date", start_date)
    parsed_end = _parse_date("end-date", end_date)

    settings = PipelineSettings()
    settings.ensure_directories()

    identifiers = [query]
    if start_date:
        identifiers.append(f"start:{start_date}")
    if end_date:
        identifiers.append(f"end:{end_date}")

    search_hash = hash_identifiers(identifiers)
    run_date = datetime.utcnow().strftime("%Y%m%d")
    run_dir = settings.raw_dir / "pubmed" / search_hash / run_date
    run_dir.mkdir(parents=True, exist_ok=True)

    service = PubMedSearchService(api_key=api_key, email=email, retmax=retmax)
    pmids = service.search_pmids(query, start_date=parsed_start, end_date=parsed_end)

    pmid_file = run_dir / "pmids.txt"
    pmid_file.write_text("\n".join(pmids))

    metadata = {
        "source": "pubmed",
        "query": query,
        "start_date": parsed_start.isoformat() if parsed_start else None,
        "end_date": parsed_end.isoformat() if parsed_end else None,
        "result_count": len(pmids),
        "run_started_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "retmax": retmax,
    }

    metadata_file = run_dir / "metadata.json"
    metadata_file.write_text(json.dumps(metadata, indent=2, sort_keys=True))

    typer.echo(f"Stored {len(pmids)} PMIDs at {pmid_file}")


@resolve_app.command("ids")
def resolve_ids(
    input_file: Path = typer.Argument(
        ..., exists=True, readable=True, help="Path to identifiers list."
    ),
) -> None:
    """Resolve PMIDs/PMCIDs/DOIs into unified records."""

    raw_lines = [line.strip() for line in input_file.read_text().splitlines()]
    identifiers = [line for line in raw_lines if line]
    if not identifiers:
        raise typer.BadParameter("Input file contains no identifiers", param_name="input_file")

    try:
        normalized = [normalize_identifier(value) for value in identifiers]
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_name="input_file") from exc

    settings = PipelineSettings()
    settings.ensure_directories()

    hashed_components = [item.hash_component for item in normalized]
    base_dir = settings.interim_dir / "resolved"
    run_dir = build_hashed_output_dir(base_dir, hashed_components)

    with IdentifierResolver() as resolver:
        result = resolver.resolve(normalized)

    records_path = run_dir / "records.jsonl"
    with records_path.open("w", encoding="utf-8") as handle:
        for record in result.records:
            handle.write(json.dumps(record.to_dict(), separators=(",", ":")))
            handle.write("\n")

    metadata = {
        "input_file": str(input_file),
        "input_count": len(identifiers),
        "unique_inputs": len({item.hash_component for item in normalized}),
        "record_count": len(result.records),
        "sources": sorted(result.sources_used),
        "errors": result.errors,
        "input_hash": run_dir.name,
        "run_started_at": result.started_at.isoformat(timespec="seconds") + "Z",
        "records_path": str(records_path),
    }

    metadata_path = run_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True))

    typer.echo(
        f"Resolved {len(identifiers)} identifiers into {len(result.records)} records at {records_path}"
    )
    if result.errors:
        typer.echo(
            f"Encountered {len(result.errors)} issue(s) during resolution; see metadata for details",
            err=True,
        )


@merge_app.command("records")
def merge_records(
    input_files: list[Path] = typer.Argument(
        ..., exists=True, readable=True, help="JSONL files produced by earlier utilities."
    ),
) -> None:
    """Merge identifier records from one or more JSONL files."""

    if not input_files:
        raise typer.BadParameter("Provide at least one JSONL file", param_name="input_files")

    settings = PipelineSettings()
    settings.ensure_directories()

    outcome = merge_jsonl_files(input_files)

    run_dir = settings.processed_dir / "merged" / outcome.input_hash
    run_dir.mkdir(parents=True, exist_ok=True)

    records_path = run_dir / "records.jsonl"
    with records_path.open("w", encoding="utf-8") as handle:
        for record in outcome.records:
            handle.write(json.dumps(record, separators=(",", ":")))
            handle.write("\n")

    metadata = {
        "input_files": outcome.source_files,
        "input_hash": outcome.input_hash,
        "record_count": len(outcome.records),
        "error_count": len(outcome.errors),
        "errors": outcome.errors,
        "run_started_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "records_path": str(records_path),
    }

    metadata_path = run_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True))

    typer.echo(
        f"Merged {len(outcome.records)} records from {len(input_files)} file(s) into {records_path}"
    )
    if outcome.errors:
        typer.echo(
            f"Encountered {len(outcome.errors)} issue(s); consult metadata for details",
            err=True,
        )


@fetch_app.command("fulltext")
def fetch_fulltext(
    records: list[Path] = typer.Argument(
        ..., exists=True, readable=True, help="Resolved/merged JSONL files to fetch full text for."
    ),
) -> None:
    """Download full text documents for the provided records."""

    if not records:
        raise typer.BadParameter("Provide at least one JSONL file", param_name="records")

    settings = PipelineSettings()
    settings.ensure_directories()

    fetcher = FullTextFetcher(settings=settings)
    result = fetcher.fetch_from_files(records)

    run_dir = settings.processed_dir / "fulltext" / result.batch_hash
    typer.echo(
        f"Fetched full text for {result.success_count} of {len(result.records)} records at {run_dir}"
    )
    if result.errors:
        typer.echo(
            f"Encountered {len(result.errors)} issue(s); see {run_dir / 'metadata.json'} for details",
            err=True,
        )
