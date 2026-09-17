from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from openfoodfacts_cluster.jobs.verify_spark_application import query_application_phase


def test_query_application_phase_selects_newest_driver() -> None:
    payload = {
        "items": [
            {
                "metadata": {"creationTimestamp": "2026-09-01T10:00:00Z"},
                "status": {"phase": "Failed"},
            },
            {
                "metadata": {"creationTimestamp": "2026-09-01T11:00:00Z"},
                "status": {"phase": "Succeeded"},
            },
        ]
    }
    response = MagicMock()
    response.__enter__.return_value.read.return_value = json.dumps(payload).encode()

    with patch("urllib.request.urlopen", return_value=response) as request:
        phase = query_application_phase(
            "https://kubernetes.default.svc",
            "openfoodfacts",
            "openfoodfacts-lab7-r2",
            "token",
            MagicMock(),
        )

    assert phase == "Succeeded"
    called_request = request.call_args.args[0]
    assert "spark-app-name%3Dopenfoodfacts-lab7-r2" in called_request.full_url
