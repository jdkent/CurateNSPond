"""Configuration for the CurateNSPond pipeline."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Directories(BaseModel):
    """Collection of canonical data directories used by the pipeline."""

    raw: Path
    interim: Path
    processed: Path
    final: Path

    def ensure(self) -> None:
        """Create directories if they do not exist."""

        for directory in (self.raw, self.interim, self.processed, self.final):
            directory.mkdir(parents=True, exist_ok=True)


class PipelineSettings(BaseSettings):
    """Settings that control runtime behavior and filesystem layout."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    data_root: Path = Field(default=Path("data"), validation_alias="CURATE_DATA_ROOT")

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def interim_dir(self) -> Path:
        return self.data_root / "interim"

    @property
    def processed_dir(self) -> Path:
        return self.data_root / "processed"

    @property
    def final_dir(self) -> Path:
        return self.data_root / "final"

    def directories(self) -> Directories:
        """Return the structured collection of key directories."""

        return Directories(
            raw=self.raw_dir,
            interim=self.interim_dir,
            processed=self.processed_dir,
            final=self.final_dir,
        )

    def ensure_directories(self) -> None:
        """Ensure that all canonical directories exist."""

        self.directories().ensure()
