from __future__ import annotations
import yaml
from pathlib import Path

def load_ingest_registry(path: str) -> dict:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "ingests" not in data:
        raise ValueError("Invalid ingest registry YAML")
    return data

def load_job_config(config_path: str) -> dict:
    p = Path(config_path)
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
