from __future__ import annotations
import logging
from dataclasses import dataclass
from sqlalchemy.engine import Engine

from core.locks import try_advisory_lock, advisory_unlock

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class JobResult:
    job_name: str
    ok: bool
    details: str = ""

def run_with_lock(engine: Engine, *, lock_name: str, fn, job_name: str) -> JobResult:
    try:
        fn()
        return JobResult(job_name=job_name, ok=True)
    except Exception as e:
        log.exception("job failed: %s", job_name)
        return JobResult(job_name=job_name, ok=False, details=str(e))
