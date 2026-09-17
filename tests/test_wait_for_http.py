from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openfoodfacts_cluster.jobs.wait_for_http import wait_for_http


def test_wait_for_http_returns_on_success() -> None:
    response = MagicMock()
    response.__enter__.return_value.status = 200

    with patch("urllib.request.urlopen", return_value=response) as request:
        wait_for_http("http://service/health", timeout_seconds=1, interval_seconds=0)

    request.assert_called_once()


def test_wait_for_http_times_out_after_failed_requests() -> None:
    with (
        patch("urllib.request.urlopen", side_effect=TimeoutError("slow")),
        patch("time.monotonic", side_effect=[0.0, 0.0, 2.0]),
        patch("time.sleep"),
        pytest.raises(TimeoutError, match="Dependency did not become ready"),
    ):
        wait_for_http("http://service/health", timeout_seconds=1, interval_seconds=0)
