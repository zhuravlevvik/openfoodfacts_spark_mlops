from __future__ import annotations

import pytest

from openfoodfacts_cluster.spark import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session("openfoodfacts-tests", "local[2]")
    yield session
    session.stop()
