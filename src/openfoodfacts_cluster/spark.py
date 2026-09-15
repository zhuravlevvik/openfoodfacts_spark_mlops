"""SparkSession construction kept in one place for consistent configuration."""

from __future__ import annotations

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name: str, master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def dense_vector_from_array(column_name: str) -> Column:
    """Convert an array column without importing Pandas-dependent ML helpers."""

    to_vector = F.udf(lambda values: Vectors.dense(values), VectorUDT())
    return to_vector(F.col(column_name))
