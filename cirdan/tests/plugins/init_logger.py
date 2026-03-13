import time
from collections.abc import Generator
from typing import Any

import pytest
from loguru import logger
from tests.setup_logger import setup_logger


def _setup_logger() -> None:
    setup_logger()


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    config_name = config.inipath
    _setup_logger()
    logger.info(f'Started logging! ({config_name})')


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]) -> Generator[None]:
    outcome = yield
    if outcome is None:
        logger.warning(f'No outcome from pytest_runtest_makereport for {item.nodeid} at phase {call.when}')
        return
    rep = outcome.get_result()
    setattr(item, 'rep_' + rep.when, rep)


@pytest.fixture(autouse=True)
def log_test_start_end(request: pytest.FixtureRequest) -> Generator[None]:
    nodeid = request.node.nodeid
    logger.info(f'STARTING TEST: {nodeid}')
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start

    status = 'UNKNOWN'
    rep_call = getattr(request.node, 'rep_call', None)
    if rep_call is not None:
        if rep_call.skipped:
            status = 'SKIPPED'
        elif rep_call.passed:
            status = 'PASSED'
        else:
            status = 'FAILED'

    logger.info(f'FINISHED TEST: {nodeid} | {status} | {elapsed:.2f}s')
