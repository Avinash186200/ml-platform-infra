"""Artifact store on top of fsspec.

``base_url`` is a local directory in tests and docker-compose
(``/data/artifacts``), ``memory://...`` in some unit tests, and
``s3://<bucket>/<prefix>`` in the cloud environment (needs the ``s3`` extra,
which installs ``s3fs``; credentials come from the pod's IAM role, never from
config). Only the local and memory backends are exercised by the tests.

Layout under ``base_url``::

    raw/prices.parquet, raw/fundamentals.parquet
    features/v000001.sqlite ...            immutable snapshots, never overwritten
    features/STAGED                        the newest snapshot, written but not yet checked/published
    features/CURRENT                       {"version": 3, "key": ..., "sha256": ..., "previous": ...}
    training/{train,test}.parquet, training/info.json
    models/candidates/<model_id>/model.json, evaluation.json
    models/CURRENT                         {"model_id": ..., "key": ..., "previous": ...}
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import uuid
from pathlib import Path

import fsspec
import pandas as pd

FEATURES_POINTER = "features/CURRENT"
FEATURES_STAGED = "features/STAGED"  # written, not yet published
FEATURES_CHECKED = "features/CHECKED"  # the version + hash the point-in-time check last passed
MODELS_POINTER = "models/CURRENT"


class ArtifactStore:
    def __init__(self, base_url: str):
        self.base_url = str(base_url).rstrip("/")
        self.fs, self.root = fsspec.core.url_to_fs(self.base_url)
        if self._is_local():  # object stores have no directories to create
            self.fs.makedirs(self.root, exist_ok=True)

    def path(self, key: str) -> str:
        """Map a key to a path under the root. Keys are relative and may not climb out of it."""
        parts = key.replace("\\", "/").split("/")
        bad = any(seg in ("", ".", "..") or ":" in seg or seg.startswith("~") or "%" in seg for seg in parts)
        if key.startswith(("/", "\\")) or bad:
            raise ValueError(f"invalid artifact key {key!r}")
        return f"{self.root}/{key}"

    def exists(self, key: str) -> bool:
        """True if the object exists. A plain read, so it needs only GetObject
        (plus ListBucket on S3, which turns a missing key into 404 instead of 403)."""
        try:
            self.fs.info(self.path(key))
        except FileNotFoundError:
            return False
        return True

    def write_bytes(self, key: str, data: bytes, overwrite: bool = True) -> None:
        """Write an object. ``overwrite=False`` refuses to replace an existing key.

        Both paths are atomic exclusive creates: ``open(..., "xb")`` on local
        disk, and on S3 an ``"xb"`` open, which s3fs turns into a conditional
        PUT (``If-None-Match: *``); S3 rejects it if the key already exists.
        """
        p = self.path(key)
        if self._is_local():
            self.fs.makedirs(p.rsplit("/", 1)[0], exist_ok=True)
            if not overwrite:
                with open(p, "xb") as fh:  # FileExistsError if the key exists
                    fh.write(data)
                return
        with self.fs.open(p, "wb" if overwrite else "xb") as fh:
            fh.write(data)

    def read_bytes(self, key: str) -> bytes:
        with self.fs.open(self.path(key), "rb") as fh:
            return fh.read()

    def write_json(self, key: str, obj) -> None:
        self.write_bytes(key, json.dumps(obj, indent=2, sort_keys=True).encode())

    def read_json(self, key: str):
        return json.loads(self.read_bytes(key))

    def write_parquet(self, key: str, df: pd.DataFrame) -> None:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self.write_bytes(key, buf.getvalue())

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self.read_bytes(key)))

    def upload_file(self, local_path: str | Path, key: str, overwrite: bool = True) -> None:
        self.write_bytes(key, Path(local_path).read_bytes(), overwrite=overwrite)

    def download_file(self, key: str, local_path: str | Path) -> None:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = local_path.with_name(local_path.name + f".part-{uuid.uuid4().hex[:8]}")
        tmp.write_bytes(self.read_bytes(key))
        tmp.replace(local_path)

    def set_pointer(self, key: str, obj: dict) -> None:
        """Write a pointer file so readers see either the old or the new one, never half of each.

        Local disk: write a temp file and ``os.replace`` it (atomic on POSIX
        and Windows). Object stores (S3) replace an object atomically on PUT,
        so a direct write is enough there.
        """
        if not self._is_local():
            self.write_json(key, obj)
            return
        tmp = f"{key}.tmp-{uuid.uuid4().hex[:8]}"
        self.write_json(tmp, obj)
        os.replace(self.path(tmp), self.path(key))

    def _is_local(self) -> bool:
        protocol = self.fs.protocol
        protocols = (protocol,) if isinstance(protocol, str) else tuple(protocol)
        return "file" in protocols or "local" in protocols

    def get_pointer(self, key: str) -> dict | None:
        try:
            return self.read_json(key)
        except FileNotFoundError:
            return None

    def list_keys(self, prefix: str) -> list[str]:
        base = self.path(prefix)
        try:
            found = self.fs.find(base)
        except FileNotFoundError:
            return []
        root = self.root + "/"
        return sorted(p.removeprefix(root) for p in found)


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


SNAPSHOT_RE = re.compile(r"^features/v(\d{6})\.sqlite$")


def snapshot_key(version: int) -> str:
    return f"features/v{version:06d}.sqlite"


def next_snapshot_version(store: ArtifactStore) -> int:
    """One more than the highest snapshot ever written, whatever CURRENT points at.

    After a rollback CURRENT points at an older version; numbering from it
    would collide with a newer file. Numbering from the files cannot.
    """
    versions = [int(m.group(1)) for k in store.list_keys("features") if (m := SNAPSHOT_RE.match(k))]
    return max(versions, default=0) + 1
