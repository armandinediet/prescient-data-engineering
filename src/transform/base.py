from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass(frozen=True)
class TransformContext:
    job_name: str
    config: dict
    run_id: str

class TransformJob(ABC):
    type_name: str
    @abstractmethod
    def run(self, ctx: TransformContext) -> None: ...
