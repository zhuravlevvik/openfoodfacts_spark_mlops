from __future__ import annotations

import pytest

from openfoodfacts_cluster.datamart import DataMartClient


class FakeResponse:
    def __init__(self, payload) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


def test_client_paginates_one_stable_dataset_version(monkeypatch) -> None:
    client = DataMartClient("http://data-mart:8081")
    pages = iter(
        [
            FakeResponse(
                {
                    "version": "v1",
                    "total": 2,
                    "items": [
                        {
                            "code": "1",
                            "product_name": "A",
                            "categories": "Food",
                            "features": [0.0, 1.0],
                        }
                    ],
                }
            ),
            FakeResponse(
                {
                    "version": "v1",
                    "total": 2,
                    "items": [
                        {
                            "code": "2",
                            "product_name": "B",
                            "categories": "Food",
                            "features": [1.0, 0.0],
                        }
                    ],
                }
            ),
        ]
    )
    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: next(pages))

    version, products = client.fetch_current(page_size=1)

    assert version == "v1"
    assert [product.code for product in products] == ["1", "2"]


def test_client_rejects_version_change_during_pagination(monkeypatch) -> None:
    client = DataMartClient("http://data-mart:8081")
    pages = iter(
        [
            FakeResponse({"version": "v1", "total": 2, "items": [{"code": "1", "features": [0]}]}),
            FakeResponse({"version": "v2", "total": 2, "items": [{"code": "2", "features": [1]}]}),
        ]
    )
    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: next(pages))

    with pytest.raises(RuntimeError, match="version changed"):
        client.fetch_current(page_size=1)
