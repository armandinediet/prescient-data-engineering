from __future__ import annotations
import time
import logging
from typing import Any, Dict, Tuple
import requests

log = logging.getLogger(__name__)

class OpenWeatherClient:
    def __init__(self, api_key: str, base_url: str = "https://api.openweathermap.org/data/2.5"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def forecast_by_city_id(self, city_id: int, units: str = "metric") -> Tuple[int, Dict[str, Any]]:
        url = f"{self.base_url}/forecast"
        params = {"id": city_id, "appid": self.api_key, "units": units}
        print(url)
        return self._get(url, params)

    def _get(self, url: str, params: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        attempts = 4
        backoff = 1.0
        last_err = None
        for i in range(attempts):
            try:
                r = requests.get(url, params=params, timeout=20)
                status = r.status_code
                if status >= 500:
                    raise RuntimeError(f"server_error status={status}")
                return status, r.json()
            except Exception as e:
                last_err = e
                log.warning("request failed attempt=%s/%s err=%s", i + 1, attempts, e)
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError(f"request failed after retries: {last_err}")
