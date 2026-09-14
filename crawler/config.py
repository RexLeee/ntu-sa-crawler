"""Single entry point for reading config.toml."""

from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.toml"


@lru_cache(maxsize=1)
def load_config() -> dict:
    with CONFIG_PATH.open("rb") as fh:
        return tomllib.load(fh)


def data_dir() -> Path:
    d = PROJECT_ROOT / load_config()["paths"]["data_dir"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def state_dir() -> Path:
    d = PROJECT_ROOT / load_config()["paths"]["state_dir"]
    d.mkdir(parents=True, exist_ok=True)
    return d
