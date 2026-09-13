"""SparkSession construction kept in one place for consistent configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession


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
