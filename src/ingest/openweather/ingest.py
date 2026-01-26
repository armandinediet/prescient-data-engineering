from __future__ import annotations

import logging

from config import load_settings
from db.engine import make_engine
from db.repository import generic_raw_insert  # <-- new generic writer
from ingest.base import IngestJob, IngestContext
from ingest.openweather.client import OpenWeatherClient

log = logging.getLogger(__name__)


class OpenWeatherForecastIngest(IngestJob):
    type_name = "openweather_forecast"

    def run(self, ctx: IngestContext) -> None:
        settings = load_settings()
        if not settings.openweather_api_key:
            raise RuntimeError("OPENWEATHER_API_KEY is required")

        engine = make_engine(settings.database_url)

        cfg = ctx.config or {}
        units = (cfg.get("units") or "metric").strip()
        cities = cfg.get("cities") or []
        if not cities:
            raise RuntimeError("No cities configured in ingests/openweather.yaml")

        # Allow override per job; fallback to one sensible default
        raw_table = (cfg.get("raw_table") or "raw_weather_forecast").strip()

        client = OpenWeatherClient(settings.openweather_api_key)

        for city in cities:
            city_id = int(city["id"])

            status_code = None
            payload = None
            err = None

            req_url = f"{client.base_url}/forecast"
            params = {"id": city_id, "units": units}

            try:
                status_code, payload = client.forecast_by_city_id(city_id=city_id, units=units)

                # OpenWeather sometimes returns cod as string
                cod = None
                if isinstance(payload, dict):
                    cod = payload.get("cod")

                if status_code != 200 or (cod is not None and str(cod) not in ("200", "0", "OK")):
                    # keep payload; mark error for audit
                    msg = payload.get("message") if isinstance(payload, dict) else None
                    err = f"api_error status={status_code} cod={cod} message={msg}"
                    log.warning(
                        "api returned non-success: city_id=%s status=%s cod=%s",
                        city_id,
                        status_code,
                        cod,
                    )

            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                log.exception("ingest failed for city_id=%s", city_id)

            # items to insert: one row per forecast item (map), not the entire payload
            items = []
            city_meta = None
            if isinstance(payload, dict) and not err:
                items = payload.get("list") or []
                city_meta = payload.get("city") or None

            result = generic_raw_insert(
                engine=engine,
                raw_table=raw_table,
                job_name=ctx.job_name,  # usually "openweather_forecast"
                payload_items=items if isinstance(items, list) else None,
                status_code=status_code,
                error=err,
                request_url=req_url,
                request_params=params,
                extra_meta={"city": city_meta, "city_id": city_id, "units": units},
                flatten_sep=".",  # produces keys like "main.temp", "wind.speed", etc.
            )

            log.info(
                "raw saved: city_id=%s status=%s err=%s request_id=%s rows=%s",
                city_id,
                status_code,
                bool(err),
                result["request_id"],
                result["rows_inserted"],
            )
