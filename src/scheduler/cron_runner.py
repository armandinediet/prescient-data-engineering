from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import yaml
from croniter import croniter
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _cron_matches_now(expr: str, now_utc: datetime) -> bool:
    now = _floor_to_minute(now_utc)
    return croniter.match(expr, now)


@dataclass(frozen=True)
class JobSpec:
    job_key: str
    job_type: str           # "ingest" or "transform"
    schedule: str
    enabled: bool
    command: List[str]
    cwd: Optional[str] = None


def build_jobs_from_registries(
    *,
    ingest_registry_path: str,
    transform_registry_path: str,
    project_root: str,
    cli_module: str = "cli",  # change to "weather_platform.cli" if that's your module
) -> List[JobSpec]:
    """
    Builds jobs that ALWAYS call the project's CLI via poetry.
    This avoids path issues (dbt project-dir/profiles-dir) and keeps behavior consistent.
    """
    jobs: List[JobSpec] = []

    ing = _load_yaml(ingest_registry_path)
    for item in (ing.get("ingests") or []):
        if not isinstance(item, dict):
            continue

        name = item.get("name")
        enabled = bool(item.get("enabled", True))
        schedule = item.get("schedule")
        if not name or not schedule:
            continue

        # Match what you tested: poetry run python -m cli ingest --job <name>
        cmd = ["poetry", "run", "python", "-m", cli_module, "ingest", "--job", str(name)]

        jobs.append(
            JobSpec(
                job_key=f"ingest:{name}",
                job_type="ingest",
                schedule=str(schedule),
                enabled=enabled,
                command=cmd,
                cwd=project_root,
            )
        )

    tr = _load_yaml(transform_registry_path)
    for item in (tr.get("transforms") or []):
        if not isinstance(item, dict):
            continue

        name = item.get("name")
        enabled = bool(item.get("enabled", True))
        schedule = item.get("schedule")
        if not name or not schedule:
            continue

        # Your working command: poetry run python -m cli transform --job <name>
        # This job name maps to registry inside your CLI (which knows selector, full_refresh, etc)
        cmd = ["poetry", "run", "python", "-m", cli_module, "transform", "--job", str(name)]

        jobs.append(
            JobSpec(
                job_key=f"transform:{name}",
                job_type="transform",
                schedule=str(schedule),
                enabled=enabled,
                command=cmd,
                cwd=project_root,
            )
        )

    return jobs


def _try_claim_run(engine: Engine, *, job_key: str, scheduled_for: datetime, command: List[str]) -> bool:
    cmd_str = " ".join(shlex.quote(x) for x in command)
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                insert into ops.scheduler_runs (job_key, scheduled_for, status, command)
                values (:job_key, :scheduled_for, 'running', :command)
                on conflict (job_key, scheduled_for) do nothing
            """),
            {"job_key": job_key, "scheduled_for": scheduled_for, "command": cmd_str},
        )
        return (res.rowcount or 0) == 1


def _finish_run(engine: Engine, *, job_key: str, scheduled_for: datetime, status: str, exit_code: int | None, error: str | None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                update ops.scheduler_runs
                set finished_at = now(),
                    status = :status,
                    exit_code = :exit_code,
                    error = :error
                where job_key = :job_key
                  and scheduled_for = :scheduled_for
            """),
            {
                "status": status,
                "exit_code": exit_code,
                "error": error,
                "job_key": job_key,
                "scheduled_for": scheduled_for,
            },
        )


def run_due_jobs_once(
    *,
    engine: Engine,
    ingest_registry_path: str,
    transform_registry_path: str,
    project_root: str,
    now_utc: Optional[datetime] = None,
    cli_module: str = "cli",
) -> Dict[str, Any]:
    now_utc = now_utc or _now_utc()
    scheduled_for = _floor_to_minute(now_utc)

    jobs = build_jobs_from_registries(
        ingest_registry_path=ingest_registry_path,
        transform_registry_path=transform_registry_path,
        project_root=project_root,
        cli_module=cli_module,
    )

    summary = {"scheduled_for": scheduled_for.isoformat(), "ran": [], "skipped": []}

    for job in jobs:
        if not job.enabled:
            summary["skipped"].append({"job": job.job_key, "reason": "disabled"})
            continue

        if not _cron_matches_now(job.schedule, now_utc):
            summary["skipped"].append({"job": job.job_key, "reason": "not_due"})
            continue

        claimed = _try_claim_run(engine, job_key=job.job_key, scheduled_for=scheduled_for, command=job.command)
        if not claimed:
            summary["skipped"].append({"job": job.job_key, "reason": "already_ran"})
            continue

        try:
            proc = subprocess.run(
                job.command,
                cwd=job.cwd,
                capture_output=True,
                text=True,
            )

            if proc.returncode == 0:
                _finish_run(engine, job_key=job.job_key, scheduled_for=scheduled_for, status="success", exit_code=0, error=None)
            else:
                err = (proc.stderr or proc.stdout or "").strip()[:4000]
                _finish_run(engine, job_key=job.job_key, scheduled_for=scheduled_for, status="failed", exit_code=proc.returncode, error=err)

        except Exception as e:
            _finish_run(engine, job_key=job.job_key, scheduled_for=scheduled_for, status="failed", exit_code=None, error=f"{type(e).__name__}: {e}")

        summary["ran"].append({"job": job.job_key})

    return summary
