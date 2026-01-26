from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence
from sqlalchemy import text
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def generic_raw_insert(
    *,
    engine: Engine,
    raw_table: str,
    job_name: str,
    payload_items: Sequence[Mapping[str, Any]] | None,
    # ids / audit
    request_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    requested_at: datetime | None = None,
    # http/audit metadata
    status_code: int | None = None,
    error: str | None = None,
    request_url: str | None = None,
    request_params: Mapping[str, Any] | None = None,
    extra_meta: Mapping[str, Any] | None = None,
    # flatten behavior
    flatten_sep: str = ".",
    flatten_max_level: int | None = None,
) -> Dict[str, Any]:
    """
    Generic raw writer. Inserts ONE row per item in `payload_items`.

    Besides storing the original `payload` (JSONB), it also stores a `flatten_payload` (JSONB)
    where nested objects are flattened into dotted keys (e.g. {"city": {"street": "x"}} -> {"city.street": "x"}).

    Expected raw_table columns (recommended):
      - id (text/uuid)
      - request_id (text/uuid)
      - run_id (text/uuid)
      - job_name (text)
      - requested_at (timestamptz)
      - status_code (int)
      - error (text)
      - request_url (text)           optional
      - request_params (jsonb)       optional
      - payload (jsonb)              required (map or null)
      - flatten_payload (jsonb)      required (map or null)
      - extra_meta (jsonb)           optional

    Notes:
      - Lists are not expanded; they remain as lists under their key.
      - Only dict nesting is flattened (safe + predictable for dbt JSON extract).
    """
    request_id = request_id or uuid.uuid4()
    run_id = run_id or uuid.uuid4()
    requested_at = requested_at or _now_utc()

    base = {
        "request_id": str(request_id),
        "run_id": str(run_id),
        "job_name": job_name,
        "requested_at": requested_at,
        "status_code": status_code,
        "error": error,
        "request_url": request_url,
        "request_params": dict(request_params) if request_params is not None else {},
        "extra_meta": dict(extra_meta) if extra_meta is not None else {},
    }

    def _flatten_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        def _walk(cur: Any, prefix: str, level: int) -> None:
            if flatten_max_level is not None and level > flatten_max_level:
                out[prefix] = cur
                return

            if isinstance(cur, dict):
                if not cur:
                    out[prefix] = {} if prefix else {}
                    return
                for k, v in cur.items():
                    key = f"{prefix}{flatten_sep}{k}" if prefix else str(k)
                    _walk(v, key, level + 1)
            else:
                # do NOT explode lists; keep as-is
                out[prefix] = cur

        _walk(d, "", 0)
        # If d was empty, _walk produced {"":{}}; normalize to {}
        if "" in out and len(out) == 1 and out[""] == {}:
            return {}
        # Remove accidental empty key
        out.pop("", None)
        return out

    rows: List[Dict[str, Any]] = []
    if payload_items:
        for it in payload_items:
            payload = dict(it) if it is not None else None
            flatten_payload = _flatten_dict(payload) if payload is not None else None
            rows.append(
                {
                    **base,
                    "id": str(uuid.uuid4()),
                    "payload": payload,
                    "flatten_payload": flatten_payload,
                }
            )
    else:
        # audit row
        rows.append(
            {
                **base,
                "id": str(uuid.uuid4()),
                "payload": None,
                "flatten_payload": None,
            }
        )

    df = pd.DataFrame(rows)

    # Ensure JSON columns exist even if caller omitted them
    for col in ("request_params", "extra_meta"):
        if col not in df.columns:
            df[col] = [{} for _ in range(len(df))]

    
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists raw"))
    
    df.to_sql(
            raw_table,
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            dtype={
                "payload": JSONB,
                "flatten_payload": JSONB,
                "request_params": JSONB,
                "extra_meta": JSONB,
            },
        )

    return {
        "request_id": request_id,
        "run_id": run_id,
        "requested_at": requested_at,
        "rows_inserted": len(df),
    }
