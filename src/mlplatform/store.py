"""Feature store snapshots: one immutable SQLite file per version.

The nightly pipeline writes a complete snapshot (``features/v000012.sqlite``)
and then moves the ``features/CURRENT`` pointer to it. Readers never see a
half-written snapshot, rollback is moving the pointer back, and a cached
answer keyed by the snapshot version can never go stale (see
``docs/feature-service.md``).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from .pit import RECORD_COLUMNS

SCHEMA = """
CREATE TABLE feature_records (
    entity       TEXT    NOT NULL,
    feature      TEXT    NOT NULL,
    event_time   INTEGER NOT NULL,
    available_at INTEGER NOT NULL,
    value        REAL    NOT NULL,
    PRIMARY KEY (entity, feature, event_time, available_at)
) WITHOUT ROWID;
CREATE INDEX ix_records_asof ON feature_records (entity, feature, available_at);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

# The as-of rule from pit.py in SQL: visible records only, then the latest
# event_time, then the latest available_at.
ASOF_SQL = """
SELECT feature, value, event_time, available_at FROM (
    SELECT feature, value, event_time, available_at,
           ROW_NUMBER() OVER (PARTITION BY feature ORDER BY event_time DESC, available_at DESC) AS rn
    FROM feature_records
    WHERE entity = ? AND available_at <= ? AND feature IN ({placeholders})
) WHERE rn = 1
"""


def write_snapshot(records: pd.DataFrame, path: str | Path, version: int) -> dict:
    """Write ``records`` to a new SQLite file at ``path`` and return its metadata."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(f"snapshot already exists: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    recs = records[RECORD_COLUMNS]
    dupes = recs.duplicated(subset=["entity", "feature", "event_time", "available_at"]).sum()
    if dupes:
        raise ValueError(f"{dupes} duplicate (entity, feature, event_time, available_at) records")
    meta = {
        "version": int(version),
        "n_records": len(recs),
        "n_entities": int(recs["entity"].nunique()),
        "features": sorted(recs["feature"].unique().tolist()),
        "max_available_at": int(recs["available_at"].max()),
        "min_available_at": int(recs["available_at"].min()),
    }
    con = sqlite3.connect(path)
    try:
        con.executescript(SCHEMA)
        con.executemany(
            "INSERT INTO feature_records VALUES (?, ?, ?, ?, ?)",
            zip(
                recs["entity"].astype(str),
                recs["feature"].astype(str),
                recs["event_time"].astype("int64").tolist(),
                recs["available_at"].astype("int64").tolist(),
                recs["value"].astype(float).tolist(),
                strict=True,
            ),
        )
        con.executemany("INSERT INTO meta VALUES (?, ?)", [(k, json.dumps(v)) for k, v in meta.items()])
        con.commit()
    finally:
        con.close()
    return meta


class SnapshotReader:
    """Read-only access to one snapshot file. Safe to share across threads."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        uri = self.path.resolve().as_uri() + "?mode=ro&immutable=1"
        self._con = sqlite3.connect(uri, uri=True, check_same_thread=False)
        self._lock = threading.Lock()
        rows = self._con.execute("SELECT key, value FROM meta").fetchall()
        self.meta = {k: json.loads(v) for k, v in rows}
        self.version: int = self.meta["version"]
        self.features: list[str] = self.meta["features"]
        self.max_available_at: int = self.meta["max_available_at"]
        self.sha256: str | None = None  # set by the loader that verified the file
        self.cache_id: str = str(self.version)  # what cache keys use; version + content hash when known
        self.queries = 0  # number of as-of lookups served from disk (for tests and the cache replay)

    def close(self) -> None:
        self._con.close()

    def has_entity(self, entity: str) -> bool:
        with self._lock:
            row = self._con.execute("SELECT 1 FROM feature_records WHERE entity = ? LIMIT 1", (entity,)).fetchone()
        return row is not None

    def entities(self) -> list[str]:
        with self._lock:
            return [r[0] for r in self._con.execute("SELECT DISTINCT entity FROM feature_records ORDER BY entity")]

    def get_asof(self, entity: str, as_of_us: int, features: Iterable[str]) -> dict:
        """``{feature: (value, event_time_us, available_at_us)}`` for features known at ``as_of_us``."""
        feats = list(features)
        if not feats:
            return {}
        sql = ASOF_SQL.format(placeholders=",".join("?" * len(feats)))
        with self._lock:
            self.queries += 1
            rows = self._con.execute(sql, (entity, int(as_of_us), *feats)).fetchall()
        return {f: (v, et, av) for f, v, et, av in rows}

    def all_records(self) -> pd.DataFrame:
        with self._lock:
            df = pd.read_sql_query("SELECT * FROM feature_records", self._con)
        return df[RECORD_COLUMNS]
