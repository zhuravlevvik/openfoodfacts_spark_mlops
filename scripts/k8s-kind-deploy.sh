#!/usr/bin/env bash
set -euo pipefail

KIND_BIN="${KIND_BIN:-kind}"
CLUSTER_NAME="${CLUSTER_NAME:-openfoodfacts-lab8}"
NAMESPACE="${NAMESPACE:-openfoodfacts}"

if ! "${KIND_BIN}" get clusters | grep -Fxq "${CLUSTER_NAME}"; then
  "${KIND_BIN}" create cluster --name "${CLUSTER_NAME}" --config config/kind-cluster.yaml
fi

"${KIND_BIN}" load docker-image --name "${CLUSTER_NAME}" \
  openfoodfacts-model:lab8 \
  openfoodfacts-data-mart:lab8 \
  openfoodfacts-hbase:lab8

NAMESPACE="${NAMESPACE}" ./scripts/k8s-acceptance.sh
