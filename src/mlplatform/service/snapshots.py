"""Keep the newest published feature snapshot open, and models/CURRENT loaded.

Both managers check their pointer at most once every ``refresh_seconds``
(``0`` = on every call, which the tests use so they never sleep). A failed
check keeps serving what is already loaded.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

from ..artifacts import FEATURES_POINTER, MODELS_POINTER, ArtifactStore, sha256_file
from ..model import LinearModel
from ..store import SnapshotReader

log = logging.getLogger(__name__)


class SnapshotManager:
    """Serves the snapshot ``features/CURRENT`` points at.

    Each downloaded file is verified against the pointer's ``sha256`` (when
    the pointer has one) and stored locally under its version and content
    hash, so a local file can never stand in for different content.
    Requests hold a reader through ``lease()``; retired readers stay open
    while leased, the two most recent are kept for instant rollback, and
    older ones are closed and their files deleted once no request uses them.
    """

    KEEP_RETIRED = 2

    def __init__(
        self,
        artifacts: ArtifactStore,
        local_dir: str | Path,
        refresh_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.artifacts = artifacts
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.refresh_seconds = refresh_seconds
        self.clock = clock
        self._reader: SnapshotReader | None = None
        self._retired: list[SnapshotReader] = []
        self._refs: dict[int, int] = {}  # id(reader) -> requests holding it
        self._last_check: float | None = None
        self._lock = threading.RLock()  # guards the reader list; never held during I/O
        self._refresh_lock = threading.Lock()  # one refresh at a time
        self.refresh_errors = 0

    def _due(self) -> bool:
        return self._last_check is None or self.clock() - self._last_check >= self.refresh_seconds

    def _matches(self, reader: SnapshotReader, version: int, sha: str | None) -> bool:
        return reader.version == version and (sha is None or reader.sha256 == sha)

    def refresh(self) -> None:
        """Single-flight: one thread checks the pointer and downloads, with no lock
        held during I/O; requests meanwhile keep using the loaded reader. Only the
        swap happens under the lock."""
        if not self._refresh_lock.acquire(blocking=False):
            return  # another thread is refreshing; keep serving what is loaded
        try:
            self._last_check = self.clock()
            try:
                pointer = self.artifacts.get_pointer(FEATURES_POINTER)
                if pointer is None:
                    return
                # A malformed pointer (missing or null version, bad key) is an error, not a crash.
                version, sha, key = int(pointer["version"]), pointer.get("sha256"), str(pointer["key"])
            except Exception:  # keep serving the loaded snapshot
                self.refresh_errors += 1
                log.exception("could not read a valid %s", FEATURES_POINTER)
                return
            with self._lock:
                if self._reader is not None and self._matches(self._reader, version, sha):
                    return
                for old in self._retired:  # rollback to a version still open
                    if self._matches(old, version, sha):
                        self._retired.remove(old)
                        if self._reader is not None:
                            self._retired.append(self._reader)
                        self._reader = old
                        log.info("serving feature snapshot v%s again", version)
                        return
            try:
                reader = self._load(key, version, sha)  # download + hash, outside the lock
            except Exception:
                self.refresh_errors += 1
                log.exception("could not load feature snapshot v%s", version)
                return
            with self._lock:
                if self._reader is not None:
                    self._retired.append(self._reader)
                self._reader = reader
            self._gc()
            log.info("serving feature snapshot v%s (%s)", version, reader.sha256[:12])
        finally:
            self._refresh_lock.release()

    def _load(self, key: str, version: int, sha: str | None) -> SnapshotReader:
        if sha is not None:
            local = self.local_dir / f"v{version:06d}-{sha[:12]}.sqlite"
            if not local.exists() or sha256_file(local) != sha:
                self.artifacts.download_file(key, local)
        else:  # a hand-written pointer without a hash: hash what we download
            tmp = self.local_dir / f"v{version:06d}-unverified.sqlite"
            self.artifacts.download_file(key, tmp)
            local = self.local_dir / f"v{version:06d}-{sha256_file(tmp)[:12]}.sqlite"
            tmp.replace(local)
        actual = sha256_file(local)
        if sha is not None and actual != sha:
            local.unlink(missing_ok=True)
            raise ValueError(f"snapshot v{version}: content hash does not match the pointer")
        reader = SnapshotReader(local)
        if reader.version != version:
            reader.close()
            raise ValueError(f"snapshot file says v{reader.version}, pointer says v{version}")
        reader.sha256 = actual
        reader.cache_id = f"{version}-{actual[:12]}"
        return reader

    def _gc(self) -> None:
        """Close and delete retired snapshots beyond the newest KEEP_RETIRED that no request holds."""
        with self._lock:
            keep = self._retired[-self.KEEP_RETIRED :] if self.KEEP_RETIRED else []
            for old in list(self._retired):
                if old in keep or self._refs.get(id(old), 0) > 0:
                    continue
                self._retired.remove(old)
                old.close()
                if old.path != (self._reader.path if self._reader else None):
                    old.path.unlink(missing_ok=True)

    @contextlib.contextmanager
    def lease(self) -> Iterator[SnapshotReader | None]:
        """The current reader, protected from cleanup until the block ends."""
        if self._due():
            self.refresh()
        with self._lock:
            reader = self._reader
            if reader is not None:
                self._refs[id(reader)] = self._refs.get(id(reader), 0) + 1
        try:
            yield reader
        finally:
            if reader is not None:
                with self._lock:
                    self._refs[id(reader)] -= 1
                    if not self._refs[id(reader)]:
                        del self._refs[id(reader)]
                self._gc()

    def current(self) -> SnapshotReader | None:
        if self._due():
            self.refresh()
        return self._reader

    def local_files(self) -> list[str]:
        return sorted(p.name for p in self.local_dir.glob("*.sqlite"))

    def close(self) -> None:
        with self._lock:
            for r in [*self._retired, *([self._reader] if self._reader else [])]:
                r.close()
            self._retired, self._reader = [], None

    @property
    def loaded_version(self) -> int | None:
        return self._reader.version if self._reader else None


class ModelRegistry:
    def __init__(
        self,
        artifacts: ArtifactStore,
        refresh_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.artifacts = artifacts
        self.refresh_seconds = refresh_seconds
        self.clock = clock
        self._current: tuple[str, LinearModel] | None = None  # swapped as one object
        self._last_check: float | None = None
        self._lock = threading.Lock()
        self.refresh_errors = 0

    def refresh(self) -> None:
        if not self._lock.acquire(blocking=False):
            return  # single-flight; callers keep the loaded model meanwhile
        try:
            self._last_check = self.clock()
            try:
                pointer = self.artifacts.get_pointer(MODELS_POINTER)
                if pointer is None or (self._current and pointer["model_id"] == self._current[0]):
                    return
                model = LinearModel.from_json(self.artifacts.read_bytes(pointer["key"]))
            except Exception:
                self.refresh_errors += 1
                log.exception("could not load %s", MODELS_POINTER)
                return
            self._current = (pointer["model_id"], model)  # one atomic assignment
            log.info("serving model %s", pointer["model_id"])
        finally:
            self._lock.release()

    def current(self) -> tuple[str, LinearModel] | None:
        if self._last_check is None or self.clock() - self._last_check >= self.refresh_seconds:
            self.refresh()
        return self._current
