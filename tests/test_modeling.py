from __future__ import annotations

import json

import pytest

from openfoodfacts_cluster.io import read_openfoodfacts_tsv
from openfoodfacts_cluster.modeling import train_and_persist


@pytest.mark.spark
def test_training_persists_model_predictions_and_metrics(spark, tmp_path) -> None:
    raw = read_openfoodfacts_tsv(spark, "data/sample/openfoodfacts.tsv")

    result = train_and_persist(
        raw_frame=raw,
        output_dir=tmp_path / "lab5",
        cluster_count=4,
        seed=42,
        max_iterations=10,
    )

    metrics = json.loads(result.metrics_path.read_text(encoding="utf-8"))
    assert result.input_rows == 42
    assert result.usable_rows == 40
    assert len(result.predictions) == 40
    assert sum(result.cluster_sizes.values()) == 40
    assert -1.0 <= result.silhouette <= 1.0
    assert metrics["cluster_count"] == 4
    assert result.model_path.is_dir()
    assert list(result.predictions_path.glob("part-*.json"))


@pytest.mark.spark
def test_training_rejects_more_clusters_than_rows(spark, tmp_path) -> None:
    raw = read_openfoodfacts_tsv(spark, "data/sample/openfoodfacts.tsv").limit(2)

    with pytest.raises(ValueError, match="needs at least 3 usable rows"):
        train_and_persist(raw, tmp_path, cluster_count=3, seed=42, max_iterations=5)
