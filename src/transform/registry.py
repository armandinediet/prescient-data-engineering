from __future__ import annotations
import yaml
from pathlib import Path

def load_transform_registry(path: str) -> dict:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "transforms" not in data:
        raise ValueError("Invalid transform registry YAML")
    return data
