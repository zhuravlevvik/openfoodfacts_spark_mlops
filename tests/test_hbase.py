from __future__ import annotations

import base64

from openfoodfacts_cluster.hbase import HBaseRestClient


class FakeResponse:
    def __init__(self, payload=None, status_code: int = 200) -> None:
        self.payload = payload or {}
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def json(self):
        return self.payload


def b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def test_put_row_encodes_row_keys_columns_and_values(monkeypatch) -> None:
    client = HBaseRestClient("http://hbase:8080")
    captured = {}

    def fake_put(url, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs["json"]
        return FakeResponse()

    monkeypatch.setattr(client.session, "put", fake_put)
    client.put_row("products", "row/1", {"info:name": "Café"})

    assert captured["url"].endswith("/products/row%2F1")
    row = captured["json"]["Row"][0]
    assert row["key"] == b64("row/1")
    assert row["Cell"][0] == {"column": b64("info:name"), "$": b64("Café")}


def test_scan_rows_decodes_hbase_json(monkeypatch) -> None:
    client = HBaseRestClient("http://hbase:8080")
    response = FakeResponse(
        {
            "Row": [
                {
                    "key": b64("123"),
                    "Cell": [{"column": b64("info:name"), "$": b64("Oats")}],
                }
            ]
        }
    )
    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: response)

    assert client.scan_rows("products", 10) == [("123", {"info:name": "Oats"})]
