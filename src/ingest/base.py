from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class IngestContext:
    job_name: str
    config: dict
    run_id: str

class IngestJob(ABC):
    type_name: str

    @abstractmethod
    def run(self, ctx: IngestContext) -> None: ...
