from __future__ import annotations

from functools import lru_cache

import pytest
from core.db import RedbookDatabase
from pipeline.retriever.tolkien_gateway_client import TolkienGatewayClient


class _ProbeDb(RedbookDatabase):
    def __init__(self) -> None:
        # Probe-only stub: no DB work needed for API status check.
        pass


@lru_cache(maxsize=1)
def _tolkien_gateway_probe_not_ok() -> bool:
    """Return True when Tolkien Gateway API probe is non-200 or fails."""
    client = TolkienGatewayClient(
        base_url='https://tolkiengateway.net',
        db=_ProbeDb(),
        timeout_seconds=10,
        pause_seconds=0.0,
        jitter_pause=False,
    )
    try:
        return client.probe_api_not_ok()
    finally:
        client.close()


@pytest.fixture(autouse=True)
def skip_http_tests_on_tolkien_gateway_probe_failure(request: pytest.FixtureRequest) -> None:
    """Skip tests marked with ``requires_http`` if API probe is not HTTP 200."""
    if request.node.get_closest_marker('requires_http') and _tolkien_gateway_probe_not_ok():
        pytest.skip('Tolkien Gateway probe was non-200; skipping live HTTP tests.')
