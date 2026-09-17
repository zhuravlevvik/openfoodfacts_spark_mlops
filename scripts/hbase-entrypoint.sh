#!/usr/bin/env bash
set -euo pipefail

advertise_host="${HBASE_ADVERTISE_HOST:-localhost}"
if [[ ! "${advertise_host}" =~ ^[a-zA-Z0-9.-]+$ ]]; then
  echo "Invalid HBASE_ADVERTISE_HOST: ${advertise_host}" >&2
  exit 1
fi
sed -i "s/__HBASE_ADVERTISE_HOST__/${advertise_host}/g" \
  "${HBASE_CONF_DIR}/hbase-site.xml"

"${HBASE_HOME}/bin/start-hbase.sh"

for attempt in $(seq 1 60); do
  if echo "status 'simple'" | "${HBASE_HOME}/bin/hbase" shell -n >/dev/null 2>&1; then
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    echo "HBase did not become ready" >&2
    exit 1
  fi
  sleep 2
done

for table in off_products_raw off_products_prepared off_datamart_meta off_cluster_results off_model_runs; do
  exists_output="$(echo "exists '${table}'" | "${HBASE_HOME}/bin/hbase" shell -n)"
  if [[ "${exists_output}" != *"true"* ]]; then
    case "${table}" in
      off_products_raw)
        echo "create '${table}', 'info', 'nutrition'" ;;
      off_products_prepared)
        echo "create '${table}', 'info', 'feature', 'meta'" ;;
      off_datamart_meta)
        echo "create '${table}', 'meta'" ;;
      off_cluster_results)
        echo "create '${table}', 'info', 'model'" ;;
      off_model_runs)
        echo "create '${table}', 'run'" ;;
    esac | "${HBASE_HOME}/bin/hbase" shell -n
  fi
done

exec "${HBASE_HOME}/bin/hbase" rest start -p 8080