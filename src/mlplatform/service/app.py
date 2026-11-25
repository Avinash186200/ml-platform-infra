"""FastAPI apps: the feature API and the model API.

Run with uvicorn's factory mode, for example::

    uvicorn --factory mlplatform.service.app:feature_app_from_env --port 8000
    uvicorn --factory mlplatform.service.app:model_app_from_env --port 8001

Configuration comes from environment variables (see ``Settings``). The Redis
password is read from the file named by ``MLPI_REDIS_PASSWORD_FILE`` (in
Kubernetes, the mounted ``redis-auth`` Secret) or, for local runs, from
``MLPI_REDIS_PASSWORD``; it is never stored in the repo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd
import redis
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field

from ..artifacts import ArtifactStore
from ..pit import to_us
from .cache import FeatureCache
from .feature_service import (
    FeatureClient,
    FeatureService,
    HttpFeatureClient,
    InProcessFeatureClient,
    NotReady,
    UnknownEntity,
    UnknownFeature,
)
from .jobs import InvalidJob, JobQueue, Worker, feature_export_handler, validate_job_key
from .snapshots import ModelRegistry, SnapshotManager


@dataclass(frozen=True)
class Settings:
    artifacts_url: str = "data/artifacts"
    snapshot_dir: str = "data/snapshot-cache"
    redis_url: str = ""  # empty = cache disabled
    redis_password: str = ""
    cache_ttl_seconds: int = 86_400
    refresh_seconds: float = 30.0
    feature_api_url: str = ""  # model API: empty = in-process feature service

    @classmethod
    def from_env(cls) -> Settings:
        e = os.environ.get
        return cls(
            artifacts_url=e("MLPI_ARTIFACTS_URL", cls.artifacts_url),
            snapshot_dir=e("MLPI_SNAPSHOT_DIR", cls.snapshot_dir),
            redis_url=e("MLPI_REDIS_URL", cls.redis_url),
            redis_password=_read_secret("MLPI_REDIS_PASSWORD"),
            cache_ttl_seconds=int(e("MLPI_CACHE_TTL_SECONDS", str(cls.cache_ttl_seconds))),
            refresh_seconds=float(e("MLPI_REFRESH_SECONDS", str(cls.refresh_seconds))),
            feature_api_url=e("MLPI_FEATURE_API_URL", cls.feature_api_url),
        )


def _read_secret(name: str) -> str:
    """``NAME_FILE`` (a mounted Secret file, preferred in Kubernetes) wins over ``NAME``."""
    path = os.environ.get(f"{name}_FILE")
    if path:
        with open(path, encoding="utf-8") as fh:
            return fh.read().strip()
    return os.environ.get(name, "")


def parse_as_of(value: str | None) -> int:
    """ISO-8601 to microseconds. No timezone means UTC; omitted means now."""
    if value is None:
        return to_us(datetime.now(UTC))
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=f"as_of is not a timestamp: {value!r}") from exc
    if pd.isna(ts):
        raise HTTPException(status_code=422, detail="as_of is not a timestamp")
    return to_us(ts)


def redis_client(settings: Settings, timeout: float) -> redis.Redis | None:
    if not settings.redis_url:
        return None
    return redis.Redis.from_url(
        settings.redis_url,
        password=settings.redis_password or None,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
    )


def build_feature_service(settings: Settings, client: redis.Redis | None = None) -> FeatureService:
    artifacts = ArtifactStore(settings.artifacts_url)
    snapshots = SnapshotManager(artifacts, settings.snapshot_dir, settings.refresh_seconds)
    # The cache client fails fast (0.2 s): a slow Redis must not slow down feature reads.
    if client is None:
        client = redis_client(settings, timeout=0.2)
    return FeatureService(snapshots, FeatureCache(client, settings.cache_ttl_seconds))


def build_job_queue(settings: Settings) -> JobQueue | None:
    """The job queue gets its own client with a few seconds of timeout: losing a
    lease renewal to a 0.2 s blip would hand a running job to another worker."""
    client = redis_client(settings, timeout=5.0)
    return JobQueue(client, kinds=JOB_KINDS) if client is not None else None


class JobRequest(BaseModel):
    kind: str = Field(description="job kind, e.g. feature_export")
    params: dict = Field(default_factory=dict)
    job_key: str | None = Field(None, description="optional idempotency key; default = hash of kind + params")


JOB_KINDS = {"feature_export"}


def create_feature_app(service: FeatureService, jobs: JobQueue | None = None) -> FastAPI:
    app = FastAPI(title="feature-api", version="1.0.0")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz():
        if service.snapshots.current() is None:
            raise HTTPException(status_code=503, detail="no feature snapshot loaded")
        return {"status": "ready", "snapshot_version": service.snapshots.loaded_version}

    @app.get("/features/{entity}")
    def get_features(
        entity: str,
        as_of: str | None = Query(None, description="ISO-8601; no timezone = UTC; omitted = now"),
        features: str | None = Query(None, description="comma-separated names; omitted = all"),
    ):
        as_of_us = parse_as_of(as_of)
        names = [f for f in features.split(",") if f] if features else None
        try:
            return service.get(entity, as_of_us, names).to_dict()
        except NotReady as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except UnknownEntity as exc:
            raise HTTPException(status_code=404, detail=f"unknown entity {exc}") from exc
        except UnknownFeature as exc:
            raise HTTPException(status_code=400, detail=f"unknown feature(s): {exc}") from exc

    @app.get("/stats")
    def stats():
        return {
            "snapshot_version": service.snapshots.loaded_version,
            "snapshot_refresh_errors": service.snapshots.refresh_errors,
            "cache": service.cache.stats(),
            "jobs": jobs.depth() if jobs else None,
        }

    @app.post("/jobs", status_code=202)
    def submit_job(req: JobRequest, response: Response):
        if jobs is None:
            raise HTTPException(status_code=503, detail="job queue not configured (needs Redis)")
        if req.kind not in JOB_KINDS:
            raise HTTPException(status_code=400, detail=f"unknown job kind {req.kind!r}")
        try:
            job, created = jobs.submit(req.kind, req.params, req.job_key)
        except InvalidJob as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if not created:
            response.status_code = 200  # already known: nothing new was queued
        return {"job_key": job["key"], "created": created, "state": job["state"]}

    @app.get("/jobs/{job_key}")
    def get_job(job_key: str):
        if jobs is None:
            raise HTTPException(status_code=503, detail="job queue not configured (needs Redis)")
        try:
            validate_job_key(job_key)
        except InvalidJob as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        job = jobs.get(job_key)
        if job is None:
            raise HTTPException(status_code=404, detail="unknown job")
        return job

    return app


def create_model_app(registry: ModelRegistry, features: FeatureClient) -> FastAPI:
    app = FastAPI(title="model-api", version="1.0.0")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz():
        current = registry.current()
        if current is None:
            raise HTTPException(status_code=503, detail="no model deployed")
        return {"status": "ready", "model_id": current[0]}

    @app.get("/predict/{entity}")
    def predict(entity: str, as_of: str | None = Query(None, description="ISO-8601; omitted = now")):
        current = registry.current()
        if current is None:
            raise HTTPException(status_code=503, detail="no model deployed")
        model_id, model = current
        as_of_us = parse_as_of(as_of)
        try:
            fv = features.fetch(entity, as_of_us, model.features)
        except NotReady as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except UnknownEntity as exc:
            raise HTTPException(status_code=404, detail=f"unknown entity {exc}") from exc
        missing = [f for f in model.features if f not in fv["features"]]
        if missing:
            raise HTTPException(
                status_code=422, detail={"reason": "features not available as of this time", "missing": missing}
            )
        values = {f: fv["features"][f]["value"] for f in model.features}
        return {
            "entity": entity,
            "as_of": fv["as_of"],
            "model_id": model_id,
            "snapshot_version": fv["snapshot_version"],
            "probability_outperform_5d": model.predict_one(values),
            "features": values,
            "feature_cache": fv["cache"],
        }

    return app


def feature_app_from_env() -> FastAPI:
    settings = Settings.from_env()
    return create_feature_app(build_feature_service(settings), build_job_queue(settings))


def worker_from_env(worker_id: str) -> Worker:
    """The job worker: same image as the feature API, run with ``mlplatform worker``."""
    settings = Settings.from_env()
    queue = build_job_queue(settings)
    if queue is None:
        raise SystemExit("MLPI_REDIS_URL is required for the job worker")
    service = build_feature_service(settings)
    handlers = {"feature_export": feature_export_handler(service, ArtifactStore(settings.artifacts_url))}
    alive = os.environ.get("MLPI_WORKER_ALIVE_FILE", "/tmp/mlpi-worker-alive")
    max_runtime = float(os.environ.get("MLPI_JOB_MAX_RUNTIME_SECONDS", "3600"))
    return Worker(queue, handlers, worker_id, alive_file=alive, max_runtime_seconds=max_runtime)


def model_app_from_env() -> FastAPI:
    settings = Settings.from_env()
    registry = ModelRegistry(ArtifactStore(settings.artifacts_url), settings.refresh_seconds)
    if settings.feature_api_url:
        client: FeatureClient = HttpFeatureClient(settings.feature_api_url)
    else:
        client = InProcessFeatureClient(build_feature_service(settings))
    return create_model_app(registry, client)
