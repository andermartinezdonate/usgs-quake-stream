"""Generic async FDSN client — reusable for all FDSN-compliant earthquake sources."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta

import httpx

from quake_stream.sources import SourceConfig

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, rpm: int):
        self.min_interval = 60.0 / max(rpm, 1)
        self._last_call = 0.0

    async def acquire(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        self._last_call = time.monotonic()


class FDSNClient:
    """Async HTTP client for FDSN event web services.

    Works with any FDSN-compliant source (USGS, EMSC, GFZ, ISC, GeoNet, etc.).
    """

    def __init__(self, config: SourceConfig):
        self.config = config
        self._rate_limiter = RateLimiter(config.rate_limit_rpm)
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.config.timeout_seconds)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def fetch_events(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        min_magnitude: float = 0.0,
    ) -> str:
        """Fetch earthquake events from the FDSN service.

        Args:
            start_time: Start of time window (UTC). Defaults to 2 hours ago.
            end_time: End of time window (UTC). Defaults to now.
            min_magnitude: Minimum magnitude filter.

        Returns:
            Raw response text (GeoJSON or pipe-delimited text depending on source config).
        """
        if start_time is None:
            start_time = datetime.now(timezone.utc) - timedelta(hours=2)
        if end_time is None:
            end_time = datetime.now(timezone.utc)

        fmt = "text" if self.config.format == "fdsn_text" else "geojson"
        params = {
            "format": fmt,
            "starttime": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": str(min_magnitude),
            "orderby": "time",
        }

        return await self._request_with_retry(params)

    async def _request_with_retry(self, params: dict) -> str:
        """Make HTTP request with exponential backoff retry."""
        client = await self._get_client()
        last_exc: Exception | None = None

        for attempt in range(self.config.max_retries + 1):
            await self._rate_limiter.acquire()
            try:
                resp = await client.get(self.config.base_url, params=params)
                # FDSN returns 204 No Content when no events match
                if resp.status_code == 204:
                    # Return empty structure based on format
                    if self.config.format == "fdsn_text":
                        return ""
                    return '{"type":"FeatureCollection","features":[]}'
                resp.raise_for_status()
                return resp.text
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_exc = exc
                if attempt < self.config.max_retries:
                    backoff = self.config.retry_backoff_base ** attempt
                    logger.warning(
                        "%s: attempt %d/%d failed (%s), retrying in %.1fs",
                        self.config.name, attempt + 1, self.config.max_retries + 1,
                        exc, backoff,
                    )
                    await asyncio.sleep(backoff)

        raise RuntimeError(
            f"{self.config.name}: all {self.config.max_retries + 1} attempts failed"
        ) from last_exc
