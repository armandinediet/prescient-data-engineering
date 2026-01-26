from __future__ import annotations
from datetime import datetime, timezone
import json
from dotenv import load_dotenv
import logging
from pathlib import Path
from src.scheduler.cron_runner import run_due_jobs_once
import typer
import time
import subprocess
import logging
from pathlib import Path
import yaml
import typer
from config import load_settings
from db.engine import make_engine
from core.logging import setup_logging
from core.job_runner import run_with_lock
from ingest.registry import load_ingest_registry, load_job_config
from ingest.openweather.ingest import OpenWeatherForecastIngest
from ingest.base import IngestContext

from transform.registry import load_transform_registry
from transform.base import TransformContext
from transform.dbt_job import DbtTransform

app = typer.Typer(no_args_is_help=True)
log = logging.getLogger(__name__)

INGEST_PLUGINS = {
    "openweather_forecast": OpenWeatherForecastIngest(),
}

TRANSFORM_PLUGINS = {
    "transform": DbtTransform(),
}

def _find_job(registry: dict, name: str, key: str) -> dict:
    jobs = registry.get(key) or []
    for j in jobs:
        if j.get("name") == name:
            return j
    raise KeyError(f"Job not found: {name}")


@app.command()
def transform(
    select: str = typer.Option(None, "--select"),
):
    import subprocess

    cmd = [
        "poetry", "run", "dbt", "run",'--project-dir','dbt',
        "--profiles-dir", "dbt",
    ]
    if select:
        cmd += ["--select", select]

    subprocess.run(cmd, check=True)

@app.command()
def ingest(job: str = typer.Option(..., help="Job name from ingests/registry.yaml")):
    run_id = setup_logging()
    settings = load_settings()
    engine = make_engine(settings.database_url)

    registry = load_ingest_registry("src/registries/ingests.yml")
    spec = _find_job(registry, job, "ingests")
    if not spec.get("enabled", True):
        raise typer.Exit(code=0)

    job_type = spec["type"]
    plugin = INGEST_PLUGINS.get(job_type)
    if not plugin:
        raise RuntimeError(f"Unknown ingest type: {job_type}")

    cfg = load_job_config(f"ingests/{spec.get('config')}") if spec.get("config") else {}
    ctx = IngestContext(job_name=spec["name"], config=cfg, run_id=run_id)

    def _run():
        plugin.run(ctx)

    res = run_with_lock(engine, lock_name=f"ingest:{ctx.job_name}", fn=_run, job_name=ctx.job_name)
    raise typer.Exit(code=0 if res.ok else 1)

@app.command()
def transform(job: str = typer.Option("weather_curated", help="Transform job name for dbt")):
    run_id = setup_logging()
    settings = load_settings()
    engine = make_engine(settings.database_url)

    registry = load_transform_registry("src/registries/transforms.yml")
    spec = _find_job(registry, job, "transforms")
    if not spec.get("enabled", True):
        raise typer.Exit(code=0)

    # Use dbt plugin regardless of name
    plugin = TRANSFORM_PLUGINS["transform"]
    ctx = TransformContext(job_name=spec["name"], config=spec, run_id=run_id)

    def _run():
        plugin.run(ctx)

    res = run_with_lock(engine, lock_name=f"transform:{ctx.job_name}", fn=_run, job_name=ctx.job_name)
    raise typer.Exit(code=0 if res.ok else 1)



@app.command("scheduler")
def scheduler(
    ingest_registry: str = typer.Option(
        "src/registries/ingests.yml",
        "--ingest-registry",
        help="Path to the ingest registry YAML",
    ),
    transform_registry: str = typer.Option(
        "src/registries/transforms.yml",
        "--transform-registry",
        help="Path to the transform registry YAML",
    ),
    project_root: str = typer.Option(
        ".",
        "--project-root",
        help="Project root to run commands from (cwd for subprocesses)",
    ),
    once: bool = typer.Option(
        True,
        "--once/--loop",
        help="Run a single tick (recommended for OS/K8s cron). --loop not implemented here.",
    ),
) -> None:
    """
    Evaluate cron expressions from registries and run due jobs.
    Intended to be invoked every minute by OS cron / Kubernetes CronJob.
    """
    if not once:
        raise typer.BadParameter(
            "--loop is intentionally not supported. Use OS cron or K8s CronJob to invoke --once every minute."
        )

    # Resolve paths (relative to where the command is run)
    ingest_registry_path = str(Path(ingest_registry).resolve())
    transform_registry_path = str(Path(transform_registry).resolve())
    project_root_path = str(Path(project_root).resolve())

    settings = load_settings()
    engine = make_engine(settings.database_url)

    summary = run_due_jobs_once(
        engine=engine,
        ingest_registry_path=ingest_registry_path,
        transform_registry_path=transform_registry_path,
        project_root=project_root_path,
    )

    # Nice stdout output for cron logs
    print(json.dumps(summary, default=str))

    ran = summary.get("ran", [])
    skipped = summary.get("skipped", [])
    log.info("scheduler tick done scheduled_for=%s ran=%s skipped=%s",
             summary.get("scheduled_for"), len(ran), len(skipped))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _sleep_until_next_minute() -> None:
    # sleep until the next minute boundary
    now = time.time()
    secs = 60 - (now % 60)
    time.sleep(secs)


@app.command("server")
def server(
    ingest_registry: str = typer.Option(
        "src/registries/ingests.yml",
        "--ingest-registry",
        help="Path to the ingest registry YAML (relative to project root by default)",
    ),
    transform_registry: str = typer.Option(
        "src/registries/transforms.yml",
        "--transform-registry",
        help="Path to the transform registry YAML (relative to project root by default)",
    ),
    project_root: str = typer.Option(
        ".",
        "--project-root",
        help="Project root (repo root). Used for resolving relative paths and subprocess cwd",
    ),
    env_file: str = typer.Option(
        ".env",
        "--env-file",
        help="Env file path (relative to project root by default)",
    ),
) -> None:
    """
    Long-running scheduler daemon (local-friendly).
    Loads .env and ticks every minute, executing due jobs from registries.
    """
    root = Path(project_root).resolve()

    # Load .env (cron doesn't do this; server does)
    env_path = (root / env_file).resolve() if not Path(env_file).is_absolute() else Path(env_file).resolve()
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
        log.info("loaded env file: %s", str(env_path))
    else:
        log.warning("env file not found: %s (continuing)", str(env_path))

    settings = load_settings()
    engine = make_engine(settings.database_url)

    ingest_path = (root / ingest_registry).resolve() if not Path(ingest_registry).is_absolute() else Path(ingest_registry).resolve()
    transform_path = (root / transform_registry).resolve() if not Path(transform_registry).is_absolute() else Path(transform_registry).resolve()

    log.info("scheduler server started root=%s", str(root))
    log.info("registries ingest=%s transform=%s", str(ingest_path), str(transform_path))

    # initial tick immediately
    while True:
        try:
            summary = run_due_jobs_once(
                engine=engine,
                ingest_registry_path=str(ingest_path),
                transform_registry_path=str(transform_path),
                project_root=str(root),
                now_utc=_now_utc(),
                cli_module="cli",  # keep consistent with your runner
            )
            print(json.dumps(summary, default=str))
            log.info("tick done scheduled_for=%s ran=%s skipped=%s",
                     summary.get("scheduled_for"),
                     len(summary.get("ran", [])),
                     len(summary.get("skipped", [])))
        except Exception:
            log.exception("scheduler tick failed")

        _sleep_until_next_minute()


if __name__ == "__main__":
    app()