from __future__ import annotations

from pathlib import Path

import pytest
import vcr


@pytest.fixture(scope="session")
def vcr_config() -> dict[str, object]:
    cassette_dir = Path(__file__).parent / "cassettes"
    return {
        "cassette_library_dir": str(cassette_dir),
        "path_transformer": vcr.VCR.ensure_suffix(".yaml"),
        "filter_headers": ["user-agent", "x-api-key", "authorization"],
        "filter_query_parameters": ["api_key"],
        "match_on": ["method", "scheme", "host", "port", "path", "query"],
        "record_mode": "once",
    }
