from __future__ import annotations

import pytest

from openfoodfacts_cluster.config import Settings


def test_settings_read_and_validate_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLUSTER_COUNT", "6")
    monkeypatch.setenv("MODEL_SEED", "7")
    monkeypatch.setenv("MAX_ITERATIONS", "12")
    monkeypatch.setenv("OUTPUT_DIR", "/tmp/result")

    settings = Settings.from_env()

    assert settings.cluster_count == 6
    assert settings.seed == 7
    assert settings.max_iterations == 12
    assert str(settings.output_dir) == "/tmp/result"


def test_settings_reject_non_positive_cluster_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLUSTER_COUNT", "0")

    with pytest.raises(ValueError, match="CLUSTER_COUNT must be positive"):
        Settings.from_env()
