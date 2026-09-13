"""Environment-driven configuration shared by local, Docker and Kubernetes runs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    "Validated runtime settings with safe defaults for the sample dataset."

    app_name: str
    input_path: str
    output_dir: Path
    master: str
    cluster_count: int
    seed: int
    max_iterations: int
    hbase_url: str
    hbase_raw_table: str
    hbase_results_table: str
    hbase_runs_table: str
    hbase_scan_limit: int

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            app_name=os.getenv("SPARK_APP_NAME", "openfoodfacts-kmeans-lab5"),
            input_path=os.getenv(
                "INPUT_PATH", "/opt/application/data/sample/openfoodfacts.tsv"
            ),
            output_dir=Path(os.getenv("OUTPUT_DIR", "/opt/application/output/lab5")),
            master=os.getenv("SPARK_MASTER", "local[2]"),
            cluster_count=_positive_int("CLUSTER_COUNT", 4),
            seed=int(os.getenv("MODEL_SEED", "42")),
            max_iterations=_positive_int("MAX_ITERATIONS", 30),
            hbase_url=os.getenv("HBASE_URL", "http://hbase:8080").rstrip("/"),
            hbase_raw_table=os.getenv("HBASE_RAW_TABLE", "off_products_raw"),
            hbase_results_table=os.getenv("HBASE_RESULTS_TABLE", "off_cluster_results"),
            hbase_runs_table=os.getenv("HBASE_RUNS_TABLE", "off_model_runs"),
            hbase_scan_limit=_positive_int("HBASE_SCAN_LIMIT", 10000),
        )
