"""HTTP client for the Scala/Spark data mart."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class PreparedProduct:
    code: str
    product_name: str
    categories: str
    features: list[float]


class DataMartClient:
    def __init__(self, base_url: str, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def healthcheck(self) -> None:
        response = self.session.get(
            f"{self.base_url}/health", timeout=self.timeout_seconds
        )
        response.raise_for_status()

    def refresh(self) -> dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/v1/datasets/refresh", timeout=self.timeout_seconds
        )
        response.raise_for_status()
        return response.json()

    def fetch_current(self, page_size: int) -> tuple[str, list[PreparedProduct]]:
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        offset = 0
        version: str | None = None
        products: list[PreparedProduct] = []
        while True:
            response = self.session.get(
                f"{self.base_url}/v1/datasets/current",
                params={"offset": offset, "limit": page_size},
                timeout=self.timeout_seconds
            )
            response.raise_for_status()
            page = response.json()

            if version is None:
                version = str(page["version"])
            elif page["version"] != version:
                raise RuntimeError("data mart version changed during pagination")

            items = [
                PreparedProduct(
                    code=str(item["code"]),
                    product_name=str(item.get("product_name") or ""),
                    categories=str(item.get("categories") or ""),
                    features=[float(value) for value in item["features"]],
                )
                for item in page["items"]
            ]

            products.extend(items)
            offset += len(items)
            if not items or offset >= int(page["total"]):
                break
        if version is None:
            raise RuntimeError("data mart returned no dataset version")

        return version, products

    def publish_results(
        self,
        run_id: str,
        created_at: str,
        silhouette: float,
        predictions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/v1/results",
            json={
                "run_id": run_id,
                "created_at": created_at,
                "silhouette": silhouette,
                "predictions": predictions,
            },
            timeout=self.timeout_seconds
        )
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> DataMartClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
