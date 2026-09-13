"""File input adapters for the standalone lab5 application."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_openfoodfacts_tsv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .csv(path)
    )
