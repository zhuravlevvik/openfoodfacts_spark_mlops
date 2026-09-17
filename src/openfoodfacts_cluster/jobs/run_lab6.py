"""Read products from HBase, train KMeans and publish completed results back."""

from __future__ import annotations

import uuid

from openfoodfacts_cluster.config import Settings
from openfoodfacts_cluster.contracts import (
    hbase_to_product,
    prediction_to_hbase,
    utc_now,
)
from openfoodfacts_cluster.hbase import HBaseRestClient
from openfoodfacts_cluster.modeling import train_and_persist
from openfoodfacts_cluster.spark import create_spark_session


def _prediction_rows(predictions: tuple[dict, ...], run_id: str, created_at: str):
    for prediction in predictions:
        row_key = f"{run_id}:{prediction['code']}"
        yield row_key, prediction_to_hbase(prediction, run_id, created_at)


def main() -> None:
    settings = Settings.from_env()
    run_id = str(uuid.uuid4())
    started_at = utc_now()
    spark = create_spark_session("openfoodfacts-kmeans-lab6", settings.master)

    try:
        with HBaseRestClient(settings.hbase_url) as client:
            client.healthcheck()
            source_rows = client.scan_rows(settings.hbase_raw_table, settings.hbase_scan_limit)
            products = [hbase_to_product(key, cells) for key, cells in source_rows]
            if not products:
                raise RuntimeError(f"HBase table {settings.hbase_raw_table} contains no products")

            client.put_row(
                settings.hbase_runs_table,
                run_id,
                {"run:status": "running", "run:started_at": started_at},
            )
            raw_frame = spark.createDataFrame(products)
            result = train_and_persist(
                raw_frame=raw_frame,
                output_dir=settings.output_dir,
                cluster_count=settings.cluster_count,
                seed=settings.seed,
                max_iterations=settings.max_iterations,
                persist_outputs=settings.persist_outputs,
            )

            completed_at = utc_now()
            published = client.put_rows(
                settings.hbase_results_table,
                _prediction_rows(result.predictions, run_id, completed_at),
            )
            client.put_row(
                settings.hbase_runs_table,
                run_id,
                {
                    "run:status": "complete",
                    "run:started_at": started_at,
                    "run:completed_at": completed_at,
                    "run:published_rows": str(published),
                    "run:silhouette": str(result.silhouette),
                },
            )
            print(
                f"Run {run_id} completed: read={len(products)}, published={published}, "
                f"silhouette={result.silhouette:.4f}"
            )
    except Exception:
        try:
            with HBaseRestClient(settings.hbase_url) as client:
                client.put_row(
                    settings.hbase_runs_table,
                    run_id,
                    {
                        "run:status": "failed",
                        "run:started_at": started_at,
                        "run:failed_at": utc_now(),
                    },
                )
        finally:
            raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
