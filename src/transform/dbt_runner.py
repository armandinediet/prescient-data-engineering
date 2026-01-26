import os
import subprocess
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def run_dbt(
    *,
    selector: str,
    project_dir: str,
    profiles_dir: str | None = None,
    full_refresh: bool = False,
    cwd: str | None = None,  # root do repo, por exemplo
) -> None:
    base = Path(cwd).resolve() if cwd else Path.cwd().resolve()

    project_path = (base / project_dir).resolve() if not Path(project_dir).is_absolute() else Path(project_dir).resolve()
    if not project_path.exists():
        raise FileNotFoundError(f"dbt project_dir not found: {project_path}")

    cmd = [
        "dbt",
        "run",
        "--selector",
        selector,
        "--project-dir",
        str(project_path),
    ]

    if profiles_dir:
        profiles_path = (base / profiles_dir).resolve() if not Path(profiles_dir).is_absolute() else Path(profiles_dir).resolve()
        if not profiles_path.exists():
            raise FileNotFoundError(f"dbt profiles_dir not found: {profiles_path}")
        cmd += ["--profiles-dir", str(profiles_path)]

    if full_refresh:
        cmd.append("--full-refresh")

    env = os.environ.copy()

    log.info("running dbt: %s (cwd=%s)", " ".join(cmd), str(base))
    subprocess.run(cmd, cwd=str(base), check=True, env=env)
