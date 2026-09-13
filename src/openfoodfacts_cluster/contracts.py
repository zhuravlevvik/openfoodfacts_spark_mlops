"""Stable mappings between application fields and HBase column qualifiers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from openfoodfacts_cluster.preprocessing import FEATURE_COLUMNS

RAW_COLUMN_MAP = {
    "product_name": "info:product_name",
    "categories": "info:categories",
    **{column: f"nutrition:{column.replace('-', '_')}" for column in FEATURE_COLUMNS},
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def product_to_hbase(row: dict[str, Any]) -> dict[str, str]:
    cells: dict[str, str] = {}
    for field, column in RAW_COLUMN_MAP.items():
        value = row.get(field)
        if value is not None and str(value).strip():
            cells[column] = str(value)
    return cells


def hbase_to_product(row_key: str, cells: dict[str, str]) -> dict[str, str | None]:
    product: dict[str, str | None] = {"code": row_key}
    for field, column in RAW_COLUMN_MAP.items():
        product[field] = cells.get(column)
    return product


def prediction_to_hbase(
    prediction: dict[str, Any], run_id: str, created_at: str
) -> dict[str, str]:
    return {
        "model:run_id": run_id,
        "model:cluster": str(prediction["cluster"]),
        "model:created_at": created_at,
        "info:product_name": str(prediction.get("product_name") or ""),
        "info:categories": str(prediction.get("categories") or ""),
    }
