from pathlib import Path

import pytest

from curate_ns_pond.settings import PipelineSettings


def test_defaults_use_data_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURATE_DATA_ROOT", raising=False)
    settings = PipelineSettings()

    assert settings.data_root == Path("data")
    assert settings.raw_dir == Path("data/raw")
    assert settings.interim_dir == Path("data/interim")
    assert settings.processed_dir == Path("data/processed")
    assert settings.final_dir == Path("data/final")


def test_environment_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURATE_DATA_ROOT", str(tmp_path))
    settings = PipelineSettings()

    assert settings.data_root == tmp_path
    assert settings.raw_dir == tmp_path / "raw"
