from __future__ import annotations

from openfoodfacts_cluster.contracts import hbase_to_product, prediction_to_hbase


def test_hbase_product_mapping_is_reversible() -> None:
    product = hbase_to_product(
        "123",
        {
            "info:product_name": "Oats",
            "info:categories": "Cereals",
            "nutrition:proteins_100g": "13",
        },
    )

    assert product["code"] == "123"
    assert product["product_name"] == "Oats"
    assert product["proteins_100g"] == "13"
    assert product["salt_100g"] is None


def test_prediction_mapping_contains_run_identity() -> None:
    cells = prediction_to_hbase(
        {"cluster": 2, "product_name": "Oats", "categories": "Cereals"},
        run_id="run-1",
        created_at="2026-09-01T00:00:00Z",
    )

    assert cells["model:run_id"] == "run-1"
    assert cells["model:cluster"] == "2"
    assert cells["model:created_at"] == "2026-09-01T00:00:00Z"
