"""Core package for the CurateNSPond pipeline."""

from importlib import metadata

__all__ = ["__version__"]

try:  # pragma: no cover - fallback for local editable installs without hatch build
    __version__ = metadata.version("curate-ns-pond")
except metadata.PackageNotFoundError:  # pragma: no cover - generated during build
    from ._version import __version__  # type: ignore
