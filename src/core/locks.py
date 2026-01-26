from __future__ import annotations
import hashlib
from sqlalchemy import text
from sqlalchemy.engine import Connection

def _lock_key(name: str) -> int:
    # Postgres advisory locks accept BIGINT.
    # We hash a string name into a stable 64-bit integer.
    h = hashlib.sha256(name.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)

def try_advisory_lock(conn: Connection, name: str) -> bool:
    key = _lock_key(name)
    return bool(conn.execute(
    text("select pg_try_advisory_lock(cast(:k as bigint))"),
    {"k": int(key) % (2**63 - 1)},
            ).scalar())

def advisory_unlock(conn: Connection, name: str) -> None:
    key = _lock_key(name)
    conn.execute(text("select pg_advisory_unlock(:k)"), {"k": key})
