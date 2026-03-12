"""HTTP helpers for ingestion scripts."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def build_url(base_url: str, params: dict[str, Any] | None = None) -> str:
    if not params:
        return base_url
    query = urllib.parse.urlencode(params, doseq=True)
    return f"{base_url}?{query}"


def fetch_bytes(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_seconds: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.5,
) -> bytes:
    last_error: Exception | None = None
    req_headers = headers or {}

    for attempt in range(1, retries + 1):
        request = urllib.request.Request(
            url=url, data=body, headers=req_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)

    if last_error is None:
        raise RuntimeError(f"Failed to fetch {url} for an unknown reason")
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_error}")


def fetch_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_seconds: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.5,
) -> dict[str, Any]:
    payload = fetch_bytes(
        url,
        method=method,
        headers=headers,
        body=body,
        timeout_seconds=timeout_seconds,
        retries=retries,
        backoff_seconds=backoff_seconds,
    )
    return json.loads(payload.decode("utf-8"))

