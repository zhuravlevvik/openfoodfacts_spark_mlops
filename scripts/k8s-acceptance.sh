#!/usr/bin/env bash
set -euo pipefail

release_name="${RELEASE_NAME:-openfoodfacts}"
namespace="${NAMESPACE:-openfoodfacts}"
chart_path="${CHART_PATH:-deploy/helm/openfoodfacts-platform}"
values_file="${VALUES_FILE:-${chart_path}/values-lab7.yaml}"
evidence_dir="${EVIDENCE_DIR:-output/lab8}"

model_repository="${MODEL_REPOSITORY:-openfoodfacts-model}"
model_tag="${MODEL_TAG:-lab8}"
data_mart_repository="${DATA_MART_REPOSITORY:-openfoodfacts-data-mart}"
data_mart_tag="${DATA_MART_TAG:-lab8}"
hbase_repository="${HBASE_REPOSITORY:-openfoodfacts-hbase}"
hbase_tag="${HBASE_TAG:-lab8}"

mkdir -p "${evidence_dir}"

helm upgrade --install "${release_name}" "${chart_path}" \
  --namespace "${namespace}" \
  --create-namespace \
  --values "${values_file}" \
  --set-string "images.model.repository=${model_repository}" \
  --set-string "images.model.tag=${model_tag}" \
  --set-string "images.dataMart.repository=${data_mart_repository}" \
  --set-string "images.dataMart.tag=${data_mart_tag}" \
  --set-string "images.hbase.repository=${hbase_repository}" \
  --set-string "images.hbase.tag=${hbase_tag}" \
  --set model.retainExecutorPods=true

instance_selector="app.kubernetes.io/instance=${release_name}"

dump_diagnostics() {
  set +e
  kubectl get pods,jobs,statefulsets,deployments,services,pvc,pdb \
    --namespace "${namespace}" --output wide \
    | tee "${evidence_dir}/resources-failure.txt"
  kubectl get events --namespace "${namespace}" \
    --sort-by=.metadata.creationTimestamp \
    | tee "${evidence_dir}/events.txt"
  kubectl describe pods --namespace "${namespace}" \
    | tee "${evidence_dir}/pods-describe.txt"
  for pod in $(kubectl get pods --namespace "${namespace}" \
    --output jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    echo "===== ${pod} ====="
    kubectl logs "${pod}" --namespace "${namespace}" \
      --all-containers --tail=300 \
      | tee "${evidence_dir}/${pod}.log"
  done
}

on_exit() {
  status=$?
  trap - EXIT
  if [[ "${status}" -ne 0 ]]; then
    dump_diagnostics
  fi
  exit "${status}"
}
trap on_exit EXIT

wait_for_job() {
  component="$1"
  timeout_seconds="$2"
  selector="${instance_selector},app.kubernetes.io/component=${component}"
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    job_name="$(kubectl get jobs --namespace "${namespace}" \
      --selector "${selector}" --sort-by=.metadata.creationTimestamp \
      --output name 2>/dev/null | tail -n 1)"
    if [[ -n "${job_name}" ]]; then
      succeeded="$(kubectl get "${job_name}" --namespace "${namespace}" \
        --output jsonpath='{.status.succeeded}')"
      failed="$(kubectl get "${job_name}" --namespace "${namespace}" \
        --output jsonpath='{.status.failed}')"
      if [[ "${succeeded:-0}" -ge 1 ]]; then
        return 0
      fi
      if [[ "${failed:-0}" -ge 1 ]]; then
        echo "${component} Job failed: ${job_name}" >&2
        return 1
      fi
    fi
    sleep 5
  done

  echo "Timed out waiting for ${component} Job" >&2
  return 1
}

wait_for_ready_pods() {
  component="$1"
  timeout_seconds="$2"
  selector="${instance_selector},app.kubernetes.io/component=${component}"
  discovery_deadline=$((SECONDS + 60))
  pod_count=0

  while (( SECONDS < discovery_deadline )); do
    pod_count="$(kubectl get pods --namespace "${namespace}" \
      --selector "${selector}" --output name 2>/dev/null \
      | wc -l | tr -d ' ')"
    if [[ "${pod_count}" -ge 1 ]]; then
      break
    fi
    sleep 2
  done
  if [[ "${pod_count}" -lt 1 ]]; then
    echo "No ${component} pod was created" >&2
    return 1
  fi

  kubectl wait --namespace "${namespace}" --timeout "${timeout_seconds}s" \
    --for=condition=ready pod --selector "${selector}"
}

kubectl get pods,jobs,statefulsets,deployments,services,pvc,pdb \
  --namespace "${namespace}" \
  --output wide | tee "${evidence_dir}/resources.txt"
helm get values "${release_name}" --namespace "${namespace}" --all \
  > "${evidence_dir}/helm-values.yaml"
helm get manifest "${release_name}" --namespace "${namespace}" \
  > "${evidence_dir}/manifest.yaml"

wait_for_ready_pods hbase 480
wait_for_job seed 720
wait_for_ready_pods data-mart 600
wait_for_job model-launcher 1260
wait_for_job model-verifier 1260

data_mart_replicas="$(kubectl get deployment --namespace "${namespace}" \
  --selector "${instance_selector},app.kubernetes.io/component=data-mart" \
  --output jsonpath='{.items[0].status.readyReplicas}')"
if [[ "${data_mart_replicas}" -ne 2 ]]; then
  echo "Expected two ready data-mart replicas, got ${data_mart_replicas}" >&2
  exit 1
fi

pvc_phase="$(kubectl get pvc --namespace "${namespace}" \
  --selector "${instance_selector}" --output jsonpath='{.items[0].status.phase}')"
if [[ "${pvc_phase}" != "Bound" ]]; then
  echo "Expected a Bound HBase PVC, got ${pvc_phase:-none}" >&2
  exit 1
fi

kubectl get pods --namespace "${namespace}" \
  --selector 'spark-role in (driver,executor)' --output wide \
  | tee "${evidence_dir}/spark-pods.txt"

driver_phase="$(kubectl get pods --namespace "${namespace}" \
  --selector 'spark-role=driver' --output jsonpath='{.items[0].status.phase}')"
if [[ "${driver_phase}" != "Succeeded" ]]; then
  echo "Expected a succeeded Spark driver pod, got ${driver_phase:-none}" >&2
  exit 1
fi

executor_count="$(kubectl get pods --namespace "${namespace}" \
  --selector 'spark-role=executor' --no-headers 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${executor_count}" -lt 2 ]]; then
  echo "Expected at least two Spark executor pods, got ${executor_count}" >&2
  exit 1
fi

failed_executors="$(kubectl get pods --namespace "${namespace}" \
  --selector 'spark-role=executor' \
  --field-selector status.phase=Failed --no-headers 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${failed_executors}" -ne 0 ]]; then
  echo "Found ${failed_executors} failed Spark executor pods" >&2
  exit 1
fi

hbase_service="$(kubectl get service --namespace "${namespace}" \
  --selector "${instance_selector},app.kubernetes.io/component=hbase" \
  --output jsonpath='{.items[0].metadata.name}')"
inspector_pod="openfoodfacts-lab8-inspector"
kubectl delete pod "${inspector_pod}" --namespace "${namespace}" \
  --ignore-not-found --wait=true
kubectl run "${inspector_pod}" --namespace "${namespace}" \
  --restart Never \
  --image "${model_repository}:${model_tag}" \
  --env "HBASE_URL=http://${hbase_service}:8080" \
  --command -- python3 -m openfoodfacts_cluster.jobs.inspect_hbase
kubectl wait --namespace "${namespace}" --timeout 5m \
  --for=jsonpath='{.status.phase}'=Succeeded "pod/${inspector_pod}"
kubectl logs "pod/${inspector_pod}" --namespace "${namespace}" \
  | tee "${evidence_dir}/hbase.txt"

grep -Fxq "off_products_raw: 40 rows" "${evidence_dir}/hbase.txt"
grep -Fxq "off_products_prepared: 40 rows" "${evidence_dir}/hbase.txt"
grep -Fxq "off_datamart_meta: 1 rows" "${evidence_dir}/hbase.txt"
grep -Fxq "off_cluster_results: 40 rows" "${evidence_dir}/hbase.txt"
grep -Fxq "off_model_runs: 1 rows" "${evidence_dir}/hbase.txt"
grep -Eq "status=complete$" "${evidence_dir}/hbase.txt"

kubectl delete pod "${inspector_pod}" --namespace "${namespace}" --wait=false
trap - EXIT
echo "Lab 8 Kubernetes acceptance passed"
