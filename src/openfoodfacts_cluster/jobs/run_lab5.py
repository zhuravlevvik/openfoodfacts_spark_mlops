"""Train the laboratory №5 model from an Open Food Facts TSV export."""

from __future__ import annotations

from openfoodfacts_cluster.config import Settings
from openfoodfacts_cluster.io import read_openfoodfacts_tsv
from openfoodfacts_cluster.modeling import train_and_persist
from openfoodfacts_cluster.spark import create_spark_session


def main() -> None:
    settings = Settings.from_env()
    spark = create_spark_session(settings.app_name, settings.master)
    try:
        raw_products = read_openfoodfacts_tsv(spark, settings.input_path)
        result = train_and_persist(
            raw_frame=raw_products,
            output_dir=settings.output_dir,
            cluster_count=settings.cluster_count,
            seed=settings.seed,
            max_iterations=settings.max_iterations,
            persist_outputs=settings.persist_outputs,
        )
        print(
            "Training completed: "
            f"input_rows={result.input_rows}, usable_rows={result.usable_rows}, "
            f"silhouette={result.silhouette:.4f}, clusters={result.cluster_sizes}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
