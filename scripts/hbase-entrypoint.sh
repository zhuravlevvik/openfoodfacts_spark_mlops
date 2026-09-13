#!/usr/bin/env bash
set -euo pipefail

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

for table in off_products_raw off_cluster_results off_model_runs; do
  if ! echo "exists '${table}'" | "${HBASE_HOME}/bin/hbase" shell -n | grep -q "true"; then
    case "${table}" in
      off_products_raw)
        echo "create '${table}', 'info', 'nutrition'" ;;
      off_cluster_results)
        echo "create '${table}', 'info', 'model'" ;;
      off_model_runs)
        echo "create '${table}', 'run'" ;;
    esac | "${HBASE_HOME}/bin/hbase" shell -n
  fi
done

exec "${HBASE_HOME}/bin/hbase" rest start -p 8080