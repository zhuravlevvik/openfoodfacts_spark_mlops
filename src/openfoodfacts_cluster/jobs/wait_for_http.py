"""Wait until a HTTP dependency returns a successful status code."""

from __future__ import annotations

import argparse
import time
import urllib.error
import urllib.request


def wait_for_http(url: str, timeout_seconds: float, interval_seconds: float) -> None:
    """Poll ``url`` until it answers with HTTP 2xx or the deadline expires."""

    deadline = time.monotonic() + timeout_seconds
    last_error = "no request has been made"

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if 200 <= response.status < 300:
                    print(f"Dependency is ready: {url}")
                    return
                last_error = f"HTTP {response.status}"
        except (urllib.error.URLError, TimeoutError) as e:
            last_error = str(e)
        time.sleep(interval_seconds)

    message = f"Dependency did not become ready in {timeout_seconds}s: {url}; {last_error}"
    raise TimeoutError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    wait_for_http(args.url, args.timeout, args.interval)


if __name__ == "__main__":
    main()
