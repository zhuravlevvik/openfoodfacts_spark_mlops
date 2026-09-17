"""Fail a Kubernetes Job when the matching Spark driver pod fails."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def query_application_phase(
    api_url: str,
    namespace: str,
    app_name: str,
    token: str,
    ssl_context: ssl.SSLContext,
) -> str | None:
    selector = f"spark-role=driver,spark-app-name={app_name}"
    query = urllib.parse.urlencode({"labelSelector": selector})
    namespace_path = urllib.parse.quote(namespace, safe="")
    request = urllib.request.Request(
        f"{api_url}/api/v1/namespaces/{namespace_path}/pods?{query}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, context=ssl_context, timeout=10) as response:
        items: list[dict[str, Any]] = json.loads(response.read()).get("items", [])
    if not items:
        return None
    newest = max(items, key=lambda item: item["metadata"]["creationTimestamp"])
    return str(newest["status"]["phase"])


def wait_for_application(
    api_url: str,
    namespace: str,
    app_name: str,
    token_path: Path,
    ca_path: Path,
    timeout_secons: float,
) -> None:
    token = token_path.read_text(encoding="utf-8").strip()
    ssl_context = ssl.create_default_context(cafile=ca_path)
    deadline = time.monotonic() + timeout_secons
    last_phase: str | None = None
    while time.monotonic() < deadline:
        last_phase = query_application_phase(
            api_url, namespace, app_name, token, ssl_context
        )
        if last_phase == "Succeeded":
            print(f"Spark application succeeded: {app_name}")
            return
        if last_phase == "Failed":
            raise RuntimeError(f"Spark application failed: {app_name}")
        time.sleep(2)
    raise TimeoutError(
        f"Spark application did not finish in {timeout_secons}s: "
        f"{app_name}; last phase={last_phase}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--timeout", type=float, default=1200.0)
    args = parser.parse_args()

    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    wait_for_application(
        api_url=f"https://{host}:{port}",
        namespace=args.namespace,
        app_name=args.app_name,
        token_path=Path("/var/run/secrets/kubernetes.io/serviceaccount/token"),
        ca_path=Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"),
        timeout_secons=args.timeout
    )


if __name__ == "__main__":
    main()
