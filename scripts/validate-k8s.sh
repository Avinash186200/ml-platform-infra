#!/usr/bin/env bash
# Render every kustomization and validate the output against the pinned
# Kubernetes schemas, offline. Needs kustomize and kubeconform on PATH and
# the schemas from scripts/fetch-k8s-schemas.sh.
set -euo pipefail

K8S_VERSION="${KUBERNETES_VERSION:-1.31.0}"
SCHEMAS="${SCHEMAS:-.cache/k8s-schemas}"
OUT="${OUT:-.cache/rendered}"
mkdir -p "$OUT"

for dir in k8s/base k8s/overlays/dev; do
  name="$(echo "$dir" | tr '/' '-')"
  kustomize build "$dir" > "$OUT/$name.yaml"
  kubeconform -strict -summary \
    -kubernetes-version "$K8S_VERSION" \
    -schema-location "$SCHEMAS/{{ .ResourceKind }}{{ .KindSuffix }}.json" \
    "$OUT/$name.yaml"
done
