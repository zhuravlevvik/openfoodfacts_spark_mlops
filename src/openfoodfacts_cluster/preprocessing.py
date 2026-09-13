"""Schema normalization and feature preparation for raw Open Food Facts rows."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURE_COLUMNS = (
    "energy-kcal_100g",
    "fat_100g",
    "saturated-fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "fiber_100g",
    "proteins_100g",
    "salt_100g",
)

FEATURE_BOUNDS = {
    "energy-kcal_100g": (0.0, 1000.0),
    "fat_100g": (0.0, 100.0),
    "saturated-fat_100g": (0.0, 100.0),
    "carbohydrates_100g": (0.0, 100.0),
    "sugars_100g": (0.0, 100.0),
    "fiber_100g": (0.0, 100.0),
    "proteins_100g": (0.0, 100.0),
    "salt_100g": (0.0, 100.0),
}

IDENTITY_COLUMNS = ("code", "product_name", "categories")


def validate_columns(frame: DataFrame) -> None:
    required = set(IDENTITY_COLUMNS + FEATURE_COLUMNS)
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"Input dataset is missing columns: {', '.join(missing)}")


def clean_raw_products(frame: DataFrame) -> DataFrame:
    """Return one typed row per product and replace invalid measurements with null."""

    validate_columns(frame)
    cleaned = frame.select(*(IDENTITY_COLUMNS + FEATURE_COLUMNS))
    cleaned = cleaned.withColumn("code", F.trim(F.col("code").cast("string")))

    for column in ("product_name", "categories"):
        cleaned = cleaned.withColumn(column, F.trim(F.col(column).cast("string")))

    for column, (minimum, maximum) in FEATURE_BOUNDS.items():
        numeric = F.regexp_replace(F.trim(F.col(column)), ",", ".'").cast("double")
        valid = numeric.isNotNull() & ~F.isnan(numeric) & numeric.between(minimum, maximum)
        cleaned = cleaned.withColumn(column, F.when(valid, numeric))

    any_measurement = reduce(
        lambda left, right: left | right,
        (F.col(column).isNotNull() for column in FEATURE_COLUMNS),
    )
    return (
        cleaned.filter(F.col("code").isNotNull() & (F.length("code") > 0))
        .filter(any_measurement)
        .dropDuplicates(["code"])
    )
