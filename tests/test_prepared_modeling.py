from __future__ import annotations

import pytest

from openfoodfacts_cluster.modeling import train_prepared_and_persist
from openfoodfacts_cluster.spark import dense_vector_from_array


@pytest.mark.spark
def test_prepared_training_uses_ready_feature_vector(spark, tmp_path) -> None:
    rows = [
        (str(index), f"Product {index}", "Food", [float(index), float(index % 3)])
        for index in range(12)
    ]
    frame = spark.createDataFrame(
        rows, ["code", "product_name", "categories", "feature_values"]
    ).withColumn("features", dense_vector_from_array("feature_values"))

    result = train_prepared_and_persist(
        frame,
        tmp_path / "lab7",
        cluster_count=3,
        seed=42,
        max_iterations=10,
        persist_outputs=False,
    )

    assert result.usable_rows == 12
    assert sum(result.cluster_sizes.values()) == 12
    assert len(result.predictions) == 12
    assert result.model_path is None
    assert result.predictions_path is None
    assert result.metrics_path is None
