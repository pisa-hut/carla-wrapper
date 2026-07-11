from __future__ import annotations

import re
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

_DISTRIBUTION_NAME = "carla-wrapper"


def wrapper_version() -> str:
    """Return the installed version, or the same version field from a source checkout."""
    try:
        value = version(_DISTRIBUTION_NAME)
    except PackageNotFoundError:
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        try:
            project_section = pyproject.read_text(encoding="utf-8").split("[project]", 1)[1]
            project_section = project_section.split("\n[", 1)[0]
        except (OSError, IndexError) as exc:
            raise RuntimeError(f"Cannot determine {_DISTRIBUTION_NAME} version") from exc
        match = re.search(r'^version\s*=\s*"([^"\n]+)"\s*$', project_section, re.MULTILINE)
        if match is None:
            raise RuntimeError(
                f"Cannot determine {_DISTRIBUTION_NAME} version from {pyproject}"
            ) from None
        value = match.group(1)

    if not value.strip():
        raise RuntimeError(f"{_DISTRIBUTION_NAME} version must not be empty")
    return value
