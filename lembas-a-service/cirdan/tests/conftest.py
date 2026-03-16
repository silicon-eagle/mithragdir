from pathlib import Path

import pytest
from loguru import logger

pytest_plugins = ['plugins.init_logger']


@pytest.fixture(scope='session', autouse=True)
def test_path() -> Path:
    path = Path(__file__).parent
    logger.info(f'Local test path: {path!r}')
    return path


@pytest.fixture(scope='session', autouse=True)
def root_path(test_path: Path) -> Path:
    root_path = test_path.parent
    logger.info(f'Project root path: {root_path!r}')
    return root_path
