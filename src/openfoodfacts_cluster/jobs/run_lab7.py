"""Train KMeans exclusively from features prepared by the Scala data mart."""

from __future__ import annotations

import uuid

from openfoodfacts_cluster.config import Settings
from openfoodfacts_cluster.contracts import utc_now
from openfoodfacts_cluster.datamart import DataMartClient
from openfoodfacts_cluster.modeling import train_prepared_and_persist
from openfoodfacts_cluster.spark import create_spark_session, dense_vector_from_array


def main() -> None:
    settings = Settings.from_env()
    spark = create_spark_session("openfoodfacts-kmeans-lab7")
    try:
        with DataMartClient(settings.data_mart_url) as client:
            client.healthcheck()
            refresh = client.refresh()
            version, products = client.fetch_current(settings.data_mart_page_size)
            if version != refresh["version"]:
                raise RuntimeError("refreshed data mart version is not current")

            frame = spark.createDataFrame(
                [
                    (product.code, product.product_name, product.categories, product.features)
                    for product in products
                ],
                ["code", "product_name", "categories", "feature_values"],
            ).withColumn("features", dense_vector_from_array("feature_values"))
            result = train_prepared_and_persist(
                prepared_frame=frame,
                output_dir=settings.output_dir,
                cluster_count=settings.cluster_count,
                seed=settings.seed,
                max_iterations=settings.max_iterations,
                persist_outputs=settings.persist_outputs,
            )

            response = client.publish_results(
                run_id=str(uuid.uuid4()),
                created_at=utc_now(),
                silhouette=result.silhouette,
                predictions=[
                    {"code": prediction["code"], "cluster": prediction["cluster"]}
                    for prediction in result.predictions
                ],
            )
            print(
                f"Data mart version {version}: trained={len(products)}, "
                f"published={response['published_rows']}, silhouette={result.silhouette:.4f}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
