#!/usr/bin/env bash
# Download the JSON schemas for exactly the kinds this repo uses, so kubeconform
# can then run offline (and fail, rather than skip, on any kind not listed).
# Pinned to one commit of yannh/kubernetes-json-schema.
set -euo pipefail

K8S_VERSION="${KUBERNETES_VERSION:-1.31.0}"
COMMIT="0c4dd99d9873f7b4c2da24c008476f5841adbe2d"
DEST="${1:-.cache/k8s-schemas}"
KINDS="configmap-v1 namespace-v1 serviceaccount-v1 service-v1 deployment-apps-v1
horizontalpodautoscaler-autoscaling-v2 poddisruptionbudget-policy-v1 cronjob-batch-v1
networkpolicy-networking-v1"

mkdir -p "$DEST"
for kind in $KINDS; do
  out="$DEST/$kind.json"
  if [ ! -s "$out" ]; then
    curl -sSfL --retry 3 -o "$out" \
      "https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${COMMIT}/v${K8S_VERSION}-standalone-strict/${kind}.json"
  fi
done
echo "schemas for Kubernetes ${K8S_VERSION} in ${DEST}"
