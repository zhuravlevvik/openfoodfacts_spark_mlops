"""Print compact row counts and run states for lab 6 verification."""

from __future__ import annotations

from openfoodfacts_cluster.config import Settings
from openfoodfacts_cluster.hbase import HBaseRestClient


def main() -> None:
    settings = Settings.from_env()
    tables = (
        settings.hbase_raw_table,
        settings.hbase_prepared_table,
        settings.hbase_metadata_table,
        settings.hbase_results_table,
        settings.hbase_runs_table,
    )
    with HBaseRestClient(settings.hbase_url) as client:
        for table in tables:
            rows = client.scan_rows(table, settings.hbase_scan_limit)
            print(f"{table}: {len(rows)} rows")
            if table == settings.hbase_runs_table:
                for row_key, cells in rows:
                    print(f"    {row_key}: status={cells.get('run:status', 'unknown')}")


if __name__ == "__main__":
    main()
