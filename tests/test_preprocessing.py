from __future__ import annotations

import pytest

from openfoodfacts_cluster.preprocessing import clean_raw_products


@pytest.mark.spark
def test_clean_raw_products_casts_filters_and_deduplicates(spark) -> None:
    frame = spark.createDataFrame(
        [
            ("1", "Valid", "Food", "100", "4,5", "1", "12", "3", "2", "5", "0.2"),
            ("1", "Duplicate", "Food", "101", "4", "1", "12", "3", "2", "5", "0.2"),
            ("2", "Partial", "Food", "1200", "-1", None, None, None, None, "8", None),
            ("", "No code", "Food", "10", "1", "1", "1", "1", "1", "1", "1"),
        ],
        [
            "code",
            "product_name",
            "categories",
            "energy-kcal_100g",
            "fat_100g",
            "saturated-fat_100g",
            "carbohydrates_100g",
            "sugars_100g",
            "fiber_100g",
            "proteins_100g",
            "salt_100g",
        ],
    )

    rows = {row["code"]: row.asDict() for row in clean_raw_products(frame).collect()}

    assert set(rows) == {"1", "2"}
    assert rows["1"]["fat_100g"] == pytest.approx(4.5)
    assert rows["2"]["energy-kcal_100g"] is None
    assert rows["2"]["fat_100g"] is None
    assert rows["2"]["proteins_100g"] == pytest.approx(8.0)


@pytest.mark.spark
def test_clean_raw_products_reports_missing_columns(spark) -> None:
    frame = spark.createDataFrame([("1",)], ["code"])

    with pytest.raises(ValueError, match="missing columns"):
        clean_raw_products(frame)
