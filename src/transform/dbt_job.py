from __future__ import annotations
import logging
from transform.base import TransformJob, TransformContext
from transform.dbt_runner import run_dbt

log = logging.getLogger(__name__)

class DbtTransform(TransformJob):
    type_name = "dbt"

    def run(self, ctx: TransformContext) -> None:
        selector = ctx.config.get("selector")
        # dbt lives in ./dbt
        run_dbt(
                selector=selector,
                project_dir="dbt",
                profiles_dir="dbt",
                full_refresh=True,
            )
