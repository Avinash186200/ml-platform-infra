# One image for every workload: feature API, model API, job worker, Dagster.
# The command decides the role (see docker-compose.yml and k8s/base).
# Pinned by digest (the multi-arch index for python:3.11.9-slim-bookworm), so a rebuild
# uses the same base layers; bump the tag and digest together.
FROM python:3.11.9-slim-bookworm@sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml constraints.txt README.md LICENSE ./
COPY src ./src
COPY deploy/dagster/dagster.yaml /data/dagster-home/dagster.yaml
# /data is where docker-compose mounts the shared artifact volume; it must be
# writable by the unprivileged user before the volume is first created.
RUN pip install -c constraints.txt ".[s3,ui]" \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin app \
    && mkdir -p /data/dagster-home \
    && chown -R 10001:10001 /data

USER 10001
EXPOSE 8000 8001 3000
CMD ["uvicorn", "--factory", "mlplatform.service.app:feature_app_from_env", "--host", "0.0.0.0", "--port", "8000"]
