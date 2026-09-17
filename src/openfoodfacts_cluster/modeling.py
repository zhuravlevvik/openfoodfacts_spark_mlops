"""KMeans training, evaluation and durable Spark model persistence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from openfoodfacts_cluster.preprocessing import FEATURE_COLUMNS, clean_raw_products


@dataclass(frozen=True)
class TrainingResult:
    input_rows: int
    usable_rows: int
    silhouette: float
    cluster_sizes: dict[int, int]
    predictions: tuple[dict, ...]
    model_path: Path | None
    predictions_path: Path | None
    metrics_path: Path | None


def _persist_predictions_and_metrics(
    predictions: DataFrame,
    model: PipelineModel | KMeans,
    output_dir: Path,
    metrics: dict,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "model"
    predictions_path = output_dir / "predictions"
    metrics_path = output_dir / "metrics.json"
    model.write().overwrite().save(str(model_path))
    (
        predictions.select(
            "code",
            "product_name",
            "categories",
            F.col("cluster").cast("integer"),
        )
        .orderBy("code")
        .coalesce(1)
        .write.mode("overwrite")
        .json(str(predictions_path))
    )
    metrics_path.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return model_path, predictions_path, metrics_path


def build_pipeline(cluster_count: int, seed: int, max_iterations: int) -> Pipeline:
    imputed_columns = [f"{column}__imputed" for column in FEATURE_COLUMNS]
    return Pipeline(
        stages=[
            Imputer(
                strategy="median",
                inputCols=list(FEATURE_COLUMNS),
                outputCols=imputed_columns,
            ),
            VectorAssembler(
                inputCols=imputed_columns,
                outputCol="unscaled_features",
                handleInvalid="error",
            ),
            StandardScaler(
                inputCol="unscaled_features",
                outputCol="features",
                withMean=True,
                withStd=True,
            ),
            KMeans(
                k=cluster_count,
                seed=seed,
                maxIter=max_iterations,
                featuresCol="features",
                predictionCol="cluster",
            ),
        ]
    )


def train_and_persist(
    raw_frame: DataFrame,
    output_dir: Path,
    cluster_count: int,
    seed: int,
    max_iterations: int,
    persist_outputs: bool = True,
) -> TrainingResult:
    input_rows = raw_frame.count()
    cleaned = clean_raw_products(raw_frame).cache()
    usable_rows = cleaned.count()
    if usable_rows < cluster_count:
        cleaned.unpersist()
        raise ValueError(
            f"KMeans needs at least {cluster_count} usable rows, got {usable_rows}"
        )

    model: PipelineModel = build_pipeline(cluster_count, seed, max_iterations).fit(cleaned)
    predictions = model.transform(cleaned).cache()
    silhouette = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    ).evaluate(predictions)
    cluster_sizes = {
        int(row["cluster"]): int(row["count"])
        for row in predictions.groupBy("cluster").count().orderBy("cluster").collect()
    }

    prediction_records = tuple(
        row.asDict(recursive=True)
        for row in predictions.select(
            "code",
            "product_name",
            "categories",
            F.col("cluster").cast("integer"),
        )
        .orderBy("code")
        .collect()
    )

    metrics = {
        "algorithm": "pyspark.ml.clustering.KMeans",
        "cluster_count": cluster_count,
        "cluster_sizes": cluster_sizes,
        "input_rows": input_rows,
        "max_iterations": max_iterations,
        "seed": seed,
        "silhouette_squared_euclidean": silhouette,
        "usable_rows": usable_rows,
    }
    model_path: Path | None = None
    predictions_path: Path | None = None
    metrics_path: Path | None = None
    if persist_outputs:
        output_dir.mkdir(parents=True, exist_ok=True)
        model_path = output_dir / "model"
        predictions_path = output_dir / "predictions"
        metrics_path = output_dir / "metrics.json"
        model.write().overwrite().save(str(model_path))
        (
            predictions.select(
                "code",
                "product_name",
                "categories",
                *FEATURE_COLUMNS,
                F.col("cluster").cast("integer"),
            )
            .orderBy("code")
            .coalesce(1)
            .write.mode("overwrite")
            .json(str(predictions_path))
        )
        metrics_path.write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    predictions.unpersist()
    cleaned.unpersist()
    return TrainingResult(
        input_rows=input_rows,
        usable_rows=usable_rows,
        silhouette=silhouette,
        cluster_sizes=cluster_sizes,
        predictions=prediction_records,
        model_path=model_path,
        predictions_path=predictions_path,
        metrics_path=metrics_path,
    )


def train_prepared_and_persist(
    prepared_frame: DataFrame,
    output_dir: Path,
    cluster_count: int,
    seed: int,
    max_iterations: int,
    persist_outputs: bool = True,
) -> TrainingResult:
    """Train only KMeans because the data mart already owns preprocessing."""

    usable_rows = prepared_frame.count()
    if usable_rows < cluster_count:
        raise ValueError(
            f"KMeans needs at least {cluster_count} prepared rows, got {usable_rows}"
        )
    estimator = KMeans(
        k=cluster_count,
        seed=seed,
        maxIter=max_iterations,
        featuresCol="features",
        predictionCol="cluster",
    )
    model = estimator.fit(prepared_frame)
    predictions = model.transform(prepared_frame).cache()
    silhouette = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    ).evaluate(predictions)
    cluster_sizes = {
        int(row["cluster"]): int(row["count"])
        for row in predictions.groupBy("cluster").count().orderBy("cluster").collect()
    }
    prediction_records = tuple(
        row.asDict(recursive=True)
        for row in predictions.select(
            "code",
            "product_name",
            "categories",
            F.col("cluster").cast("integer"),
        )
        .orderBy("code")
        .collect()
    )
    metrics = {
        "algorithm": "pyspark.ml.clustering.KMeans",
        "cluster_count": cluster_count,
        "cluster_sizes": cluster_sizes,
        "input_rows": usable_rows,
        "max_iterations": max_iterations,
        "preprocessing_owner": "scala-data-mart",
        "seed": seed,
        "silhouette_squared_euclidean": silhouette,
        "usable_rows": usable_rows,
    }
    model_path: Path | None = None
    predictions_path: Path | None = None
    metrics_path: Path | None = None
    if persist_outputs:
        model_path, predictions_path, metrics_path = _persist_predictions_and_metrics(
            predictions, model, output_dir, metrics
        )
    predictions.unpersist()
    return TrainingResult(
        input_rows=usable_rows,
        usable_rows=usable_rows,
        silhouette=silhouette,
        cluster_sizes=cluster_sizes,
        predictions=prediction_records,
        model_path=model_path,
        predictions_path=predictions_path,
        metrics_path=metrics_path,
    )
