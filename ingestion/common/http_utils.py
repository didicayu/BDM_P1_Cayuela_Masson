"""HTTP helpers for ingestion scripts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


@dataclass
class DownloadResult:
    source_url: str
    final_url: str
    destination: Path
    content_type: str
    size_bytes: int


def build_url(base_url: str, params: dict[str, Any] | None = None) -> str:
    if not params:
        return base_url
    query = urllib.parse.urlencode(params, doseq=True)
    return f"{base_url}?{query}"


def _open_url(
    url: str,
    *,
    method: str,
    headers: dict[str, str] | None,
    body: bytes | None,
    timeout_seconds: int,
):
    request = urllib.request.Request(
        url=url,
        data=body,
        headers=headers or {},
        method=method,
    )
    return urllib.request.urlopen(request, timeout=timeout_seconds)


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

    for attempt in range(1, retries + 1):
        try:
            with _open_url(
                url,
                method=method,
                headers=headers,
                body=body,
                timeout_seconds=timeout_seconds,
            ) as response:
                return response.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)

    if last_error is None:
        raise RuntimeError(f"Failed to fetch {url} for an unknown reason")
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_error}")


def fetch_text(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_seconds: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.5,
    encoding: str = "utf-8",
) -> str:
    payload = fetch_bytes(
        url,
        method=method,
        headers=headers,
        body=body,
        timeout_seconds=timeout_seconds,
        retries=retries,
        backoff_seconds=backoff_seconds,
    )
    return payload.decode(encoding, errors="replace")


def download_to_file(
    url: str,
    destination: Path,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_seconds: int = 60,
    retries: int = 3,
    backoff_seconds: float = 1.5,
    chunk_size: int = 1024 * 1024,
) -> DownloadResult:
    last_error: Exception | None = None
    destination.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retries + 1):
        try:
            with _open_url(
                url,
                method=method,
                headers=headers,
                body=body,
                timeout_seconds=timeout_seconds,
            ) as response:
                size_bytes = 0
                with destination.open("wb") as file:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        file.write(chunk)
                        size_bytes += len(chunk)

                content_type = response.headers.get_content_type() or ""
                return DownloadResult(
                    source_url=url,
                    final_url=response.geturl(),
                    destination=destination,
                    content_type=content_type,
                    size_bytes=size_bytes,
                )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if destination.exists():
                destination.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)

    if last_error is None:
        raise RuntimeError(f"Failed to download {url} for an unknown reason")
    raise RuntimeError(f"Failed to download {url} after {retries} attempts: {last_error}")


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
