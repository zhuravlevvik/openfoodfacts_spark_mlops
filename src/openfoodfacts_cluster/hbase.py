"""Small HBase REST client with explicit encoding, retries and scan limits."""

from __future__ import annotations

import base64
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _encode(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _decode(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


class HBaseRestClient:
    def __init__(self, base_url: str, timeout_seconds: float = 15.0) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "PUT", "POST", "DELETE"}),
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def healthcheck(self) -> None:
        response = self.session.get(
            f"{self.base_url}/version/cluster",
            headers={"Accept": "application/json"},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

    def put_row(self, table: str, row_key: str, cells: dict[str, str]) -> None:
        if not cells:
            raise ValueError("HBase mutation must contain at least one cell")
        payload = {
            "Row": [
                {
                    "key": _encode(row_key),
                    "Cell": [
                        {"column": _encode(column), "$": _encode(value)}
                        for column, value in sorted(cells.items())
                    ],
                }
            ]
        }
        url = f"{self.base_url}/{quote(table, safe='')}/{quote(row_key, safe='')}"
        response = self.session.put(
            url,
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

    def scan_rows(self, table: str, limit: int) -> list[tuple[str, dict[str, str]]]:
        if limit <= 0:
            raise ValueError("scan limit must be positive")
        url = f"{self.base_url}/{quote(table, safe='')}/*"
        response = self.session.get(
            url,
            params={"limit": limit},
            headers={"Accept": "application/json"},
            timeout=self.timeout_seconds,
        )
        if response.status_code == 404:
            return []
        response.raise_for_status()

        decoded: list[tuple[str, dict[str, str]]] = []
        for raw_row in response.json().get("Row", []):
            row_key = _decode(raw_row["key"])
            cells = {
                _decode(cell["column"]): _decode(cell["$"])
                for cell in raw_row.get("Cell", [])
            }
            decoded.append((row_key, cells))
        return decoded

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> HBaseRestClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
