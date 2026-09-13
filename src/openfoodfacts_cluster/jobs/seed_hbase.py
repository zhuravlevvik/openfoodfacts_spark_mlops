"""Load the reproducible Open Food Facts sample into the raw HBase table."""

from __future__ import annotations

import csv
import os

from openfoodfacts_cluster.config import Settings
from openfoodfacts_cluster.contracts import product_to_hbase
from openfoodfacts_cluster.hbase import HBaseRestClient


def main() -> None:
    settings = Settings.from_env()
    input_path = os.getenv("SEED_INPUT_PATH", "/opt/application/data/sample/openfoodfacts.tsv")
    with open(input_path, encoding="utf-8", newline="") as stream:
        products = list(csv.DictReader(stream, delimiter="\t"))

    rows = (
        (str(product["code"]).strip(), product_to_hbase(product))
        for product in products
        if str(product.get("code") or "").strip()
    )
    with HBaseRestClient(settings.hbase_url) as client:
        client.healthcheck()
        count = client.put_rows(settings.hbase_raw_table, rows)

    print(f"Seeded {count} source rows into {settings.hbase_raw_table}")


if __name__ == "__main__":
    main()
